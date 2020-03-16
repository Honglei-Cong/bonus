package eth

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"bytes"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	ethComm "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
	common2 "github.com/ontio/bonus/common"
	"github.com/ontio/bonus/config"
	"github.com/ontio/bonus/manager/transfer"
	"github.com/ontio/bonus/utils"
	"github.com/ontio/ontology/common/log"
	"os"
)

var (
	DEFAULT_GAS_PRICE = utils.ToIntByPrecise("0.00000004", config.ETH_DECIMALS) // 40 Gwei
	MIN_ETH_BANALNCE  = utils.ToIntByPrecise("0.00001", config.ETH_DECIMALS)
)

type Token struct {
	ContractAddr ethComm.Address
	Contract     *Erc20
	Decimals     uint64
}

type EthManager struct {
	keyStore     *keystore.KeyStore
	account      accounts.Account
	ethClient    *ethclient.Client
	txTimeClient *rpc.Client
	tokens       map[string]*Token
	Erc20Abi     abi.ABI
	nonce        uint64
	txHandleTask *transfer.TxHandleTask
	cfg          *config.Eth
	lock         sync.RWMutex
	eatp         *common2.ExcelParam
}

func NewEthManager(cfg *config.Eth, eatp *common2.ExcelParam) (*EthManager, error) {
	if cfg.RpcAddr == "" {
		return nil, fmt.Errorf("RpcAddr config error")
	}

	ethClient, err := ethclient.Dial(cfg.RpcAddr)
	if err != nil {
		return nil, fmt.Errorf("NewEthManager: connect to node failed, %s", err)
	}
	c, err := rpc.DialContext(context.Background(), cfg.RpcAddr)
	if err != nil {
		return nil, err
	}
	err = common2.CheckPath(cfg.KeyStore)
	if err != nil {
		return nil, err
	}
	if cfg.KeyStore == "" {
		cfg.KeyStore = fmt.Sprintf("%s%s%s", config.DefaultWalletPath, string(os.PathSeparator), "eth")
	}
	walletPath := fmt.Sprintf("%s%s%s%s", cfg.KeyStore, string(os.PathSeparator), "eth_", eatp.EventType)
	log.Infof("eth wallet path: %s", walletPath)

	keyStore := keystore.NewKeyStore(walletPath, keystore.StandardScryptN, keystore.StandardScryptP)
	accs := keyStore.Accounts()
	var account accounts.Account
	if len(accs) == 0 {
		account, err = keyStore.NewAccount(config.PASSWORD)
		if err != nil {
			return nil, err
		}
	} else {
		account = accs[0]
	}
	log.Infof("eth admin address: %s", account.Address.Hex())
	mgr := &EthManager{
		tokens:       make(map[string]*Token),
		account:      account,
		keyStore:     keyStore,
		cfg:          cfg,
		txTimeClient: c,
		eatp:         eatp,
	}

	nonce, err := ethClient.PendingNonceAt(context.Background(), account.Address)
	if err != nil {
		return nil, fmt.Errorf("NewEthManager: fetch nonce failed, %s", err)
	}
	mgr.nonce = nonce
	mgr.ethClient = ethClient

	mgr.Erc20Abi, err = abi.JSON(strings.NewReader(Erc20ABI))
	if err != nil {
		return nil, fmt.Errorf("NewEthManager: parse erc20 abi failed, %s", err)
	}
	return mgr, nil
}

func (self *EthManager) VerifyAddress(address string) bool {
	boo := ethComm.IsHexAddress(address)
	if !boo {
		log.Errorf("eth VerifyAddress failed, address: %s", address)
		return false
	}
	return true
}

func (self *EthManager) GetAdminAddress() string {
	return self.account.Address.Hex()
}

func (self *EthManager) GetAdminBalance() (string, error) {
	if self.eatp.TokenType == config.ERC20 {
		erc20, ok := self.tokens[self.eatp.ContractAddress]
		if !ok {
			return "", fmt.Errorf("Withdraw: token %s not exist", self.eatp.ContractAddress)
		}
		balance, err := erc20.Contract.BalanceOf(&bind.CallOpts{Pending: false}, self.account.Address)
		if err != nil {
			return "", fmt.Errorf("Withdraw: cannot get self balance, token %s, err: %s", self.eatp.ContractAddress, err)
		}
		return utils.ToStringByPrecise(balance, erc20.Decimals), nil
	}
	return "", fmt.Errorf("not support token type: %s", self.eatp.TokenType)
}

func (self *EthManager) EstimateFee() (string, error) {

	contractAddr := ethComm.HexToAddress(self.eatp.ContractAddress)
	adminAddr := self.GetAdminAddress()
	adminAddress := ethComm.HexToAddress(adminAddr)
	amount := utils.ToIntByPrecise("2000", config.ETH_DECIMALS)

	gaslimit, err := self.estimateGasLimit(contractAddr, adminAddress, amount, DEFAULT_GAS_PRICE)
	if err != nil {
		return "", err
	}
	gasLimi := new(big.Int).SetUint64(gaslimit)
	gas := gasLimi.Mul(gasLimi, DEFAULT_GAS_PRICE)
	gasTotal := gas.Mul(gas, new(big.Int).SetUint64(uint64(len(self.eatp.BillList))))
	return utils.ToStringByPrecise(gasTotal, config.ETH_DECIMALS), nil
}

func (self *EthManager) ComputeSum() (string, error) {
	if self.eatp.TokenType == config.ERC20 {
		sum := new(big.Int)
		for _, item := range self.eatp.BillList {
			val := utils.ToIntByPrecise(item.Amount, self.tokens[self.eatp.ContractAddress].Decimals)
			sum = sum.Add(sum, val)
		}
		return utils.ToStringByPrecise(sum, self.tokens[self.eatp.ContractAddress].Decimals), nil
	}
	return "", fmt.Errorf("not supported token type: %s", self.eatp.TokenType)
}

func (self *EthManager) WithdrawToken(address string) error {
	if self.eatp.TokenType == config.ERC20 {
		ba, err := self.GetAdminBalance()
		if err != nil {
			return err
		}
		hash, txHex, err := self.NewWithdrawTx(address, ba)
		if hash == "" || txHex == nil || err != nil {
			return fmt.Errorf("NewWithdrawTx failed, error: %s", err)
		}
		hash, err = self.SendTx(txHex)
		if err != nil {
			return fmt.Errorf("send tx failed, error:%s， nonce: %d", err, self.nonce)
		}
		boo := self.VerifyTx(hash)
		if !boo {
			return fmt.Errorf("verify tx failed")
		}
	}
	return nil
}

func (this *EthManager) SetContractAddress(address string) error {
	contractAddr := ethComm.HexToAddress(address)
	erc20, err := NewErc20(contractAddr, this.ethClient)
	if err != nil {
		return fmt.Errorf("NewEthManager: new erc20 contract failed, token %s, err: %s", this.eatp.ContractAddress, err)
	}
	decimals, err := erc20.Decimals(&bind.CallOpts{})
	if err != nil {
		return fmt.Errorf("NewEthManager: cannot get %s decimals, err: %s", this.eatp.ContractAddress, err)
	}
	this.tokens[this.eatp.ContractAddress] = &Token{ContractAddr: contractAddr, Decimals: decimals.Uint64(), Contract: erc20}
	return nil
}

func (self *EthManager) AppendParam(param *common2.TransferParam) {
	self.txHandleTask.TransferQueue <- param
}

func (self *EthManager) StartTransfer() {
	self.StartHandleTxTask()
	go func() {
		for _, trParam := range self.eatp.BillList {
			if trParam.Amount == "0" {
				continue
			}
			self.txHandleTask.TransferQueue <- trParam
		}
		close(self.txHandleTask.TransferQueue)
		self.txHandleTask.WaitClose()
	}()
}

func (self *EthManager) GetStatus() common2.TransferStatus {
	if self.txHandleTask == nil {
		return common2.NotTransfer
	}
	return self.txHandleTask.TransferStatus
}

func (self *EthManager) StartHandleTxTask() {
	//start transfer task and verify task
	self.txHandleTask = transfer.NewTxHandleTask(self.eatp.TokenType)
	go self.txHandleTask.StartHandleTransferTask(self, self.eatp.EventType)
	go self.txHandleTask.StartVerifyTxTask(self)
}

func (this *EthManager) NewWithdrawTx(destAddr string, amount string) (string, []byte, error) {
	to := ethComm.Address{}
	if ethComm.IsHexAddress(destAddr) {
		to = ethComm.HexToAddress(destAddr)
	} else {
		return "", nil, fmt.Errorf("Withdraw: dest addr is invaild")
	}

	ethBalance, err := this.ethClient.PendingBalanceAt(context.Background(), this.account.Address)
	if err != nil {
		return "", nil, fmt.Errorf("Withdraw: cannot get eth pending balance, err: %s", err)
	}
	if ethBalance.Cmp(MIN_ETH_BANALNCE) < 0 {
		return "", nil, fmt.Errorf("Withdraw: self eth pending balance %s not enough",
			utils.ToStringByPrecise(ethBalance, config.ETH_DECIMALS))
	}
	if this.eatp.TokenType == config.ETH {
		withdrawAmount := utils.ToIntByPrecise(amount, config.ETH_DECIMALS)
		if ethBalance.Cmp(withdrawAmount) < 0 {
			return "", nil, fmt.Errorf("Withdraw: self eth pending balance %s not enough",
				utils.ToStringByPrecise(ethBalance, config.ETH_DECIMALS))
		}
		log.Debugf("Withdraw: %s, pending balance is %s", this.eatp.TokenType,
			utils.ToStringByPrecise(ethBalance, config.ETH_DECIMALS))
		return this.newWithdrawEthTx(to, withdrawAmount, DEFAULT_GAS_PRICE)
	} else {
		erc20, ok := this.tokens[this.eatp.ContractAddress]
		if !ok {
			return "", nil, fmt.Errorf("Withdraw: token %s not exist", this.eatp.ContractAddress)
		}
		withdrawAmount := utils.ToIntByPrecise(amount, erc20.Decimals)
		balance, err := erc20.Contract.BalanceOf(&bind.CallOpts{Pending: false}, this.account.Address)
		if err != nil {
			return "", nil, fmt.Errorf("Withdraw: cannot get self balance, token %s, err: %s", this.eatp.ContractAddress, err)
		}
		if balance.Cmp(withdrawAmount) < 0 {
			return "", nil, fmt.Errorf("Withdraw: self pending balance %s not enough, token %s",
				utils.ToStringByPrecise(balance, erc20.Decimals), this.eatp.ContractAddress)
		}
		log.Debugf("Withdraw: %s, pending balance is %s", this.eatp.ContractAddress, utils.ToStringByPrecise(balance, erc20.Decimals))
		return this.newWithdrawErc20Tx(erc20.ContractAddr, to, withdrawAmount, DEFAULT_GAS_PRICE)
	}
}

func (this *EthManager) estimateGasLimit(contractAddr, to ethComm.Address, amount *big.Int, gasPrice *big.Int) (uint64, error) {
	if this.eatp.TokenType == config.ETH {
		callMsg := ethereum.CallMsg{
			From: this.account.Address, To: &to, Gas: 0, GasPrice: gasPrice,
			Value: amount, Data: []byte{},
		}
		gasLimit, err := this.ethClient.EstimateGas(context.Background(), callMsg)
		if err != nil {
			return 0, fmt.Errorf("newWithdrawEthTx: pre-execute failed, err: %s", err)
		}
		return gasLimit, nil
	} else {
		txData, err := this.Erc20Abi.Pack("transfer", to, amount)
		if err != nil {
			return 0, fmt.Errorf("newWithdrawErc20Tx: pack tx data failed, err: %s", err)
		}
		to := ethComm.HexToAddress("0xd46e8dd67c5d32be8058bb8eb970870f07244567")
		callMsg := ethereum.CallMsg{
			From: this.account.Address, To: &to, Gas: 0, GasPrice: gasPrice,
			Value: big.NewInt(0), Data: txData,
		}
		gasLimit, err := this.ethClient.EstimateGas(context.Background(), callMsg)
		if err != nil {
			return 0, fmt.Errorf("newWithdrawErc20Tx: pre-execute failed, err: %s", err)
		}
		gasLimit = gasLimit * 10
		return gasLimit, nil
	}
}

func (this *EthManager) newWithdrawErc20Tx(contractAddr, to ethComm.Address, amount *big.Int, gasPrice *big.Int) (string, []byte, error) {
	txData, err := this.Erc20Abi.Pack("transfer", to, amount)
	if err != nil {
		return "", nil, fmt.Errorf("newWithdrawErc20Tx: pack tx data failed, err: %s", err)
	}
	gasLimit, err := this.estimateGasLimit(contractAddr, to, amount, gasPrice)
	if err != nil {
		return "", nil, fmt.Errorf("EstimateGasLimit error:%s", err)
	}
	return this.newTx(contractAddr, big.NewInt(0), gasLimit, gasPrice, txData)
}

func (this *EthManager) newWithdrawEthTx(to ethComm.Address, amount *big.Int, gasPrice *big.Int) (string, []byte, error) {
	gasLimit, err := this.estimateGasLimit(to, to, amount, gasPrice)
	if err != nil {
		return "", nil, fmt.Errorf("EstimateGasLimit error:%s", err)
	}
	return this.newTx(to, amount, gasLimit, gasPrice, []byte{})
}

func (this *EthManager) newTx(to ethComm.Address, value *big.Int, gasLimit uint64, gasPrice *big.Int,
	txData []byte) (string, []byte, error) {
	err := this.keyStore.TimedUnlock(this.account, config.PASSWORD, time.Minute)
	if err != nil {
		return "", nil, fmt.Errorf("newTx: unlock account failed, err: %s", err)
	}
	tx := types.NewTransaction(this.nonce, to, value, gasLimit, gasPrice, txData)
	signedTx, err := this.keyStore.SignTx(this.account, tx, nil)
	if err != nil {
		return "", nil, fmt.Errorf("newTx: sign tx failed, err: %s", err)
	}
	log.Debugf("newTx: account %s, hash %s, nonce %d", this.account.Address.String(), signedTx.Hash().String(),
		this.nonce)
	this.nonce++
	txBuffer := new(bytes.Buffer)
	err = signedTx.EncodeRLP(txBuffer)
	if err != nil {
		return "", nil, fmt.Errorf("EncodeRLP error: %s", err)
	}
	return signedTx.Hash().String(), txBuffer.Bytes(), nil
}

type rpcTransaction struct {
	tx *types.Transaction
	txExtraInfo
}

type txExtraInfo struct {
	BlockNumber *string         `json:"blockNumber,omitempty"`
	BlockHash   *common.Hash    `json:"blockHash,omitempty"`
	From        *common.Address `json:"from,omitempty"`
}

func (this *EthManager) GetTxTime(txHash string) (uint32, error) {
	var jsonRes *rpcTransaction
	err := this.txTimeClient.CallContext(context.Background(), &jsonRes, "eth_getTransactionByHash", txHash)
	if err != nil {
		return 0, err
	} else if jsonRes == nil {
		return 0, fmt.Errorf("jsonRes is nil")
	}
	block, err := this.ethClient.BlockByHash(context.Background(), *jsonRes.BlockHash)
	if err != nil {
		return 0, err
	}
	return uint32(block.Time()), nil
}

type EthRes struct {
	Status  string   `json:"status"`
	Message string   `json:"message"`
	Result  []*TxRes `json:"result"`
}

type TxRes struct {
	BlockNumber string `json:"blockNumber"`
	TimeStamp   string `json:timeStamp`
}

func (this *EthManager) SendTx(txHex []byte) (string, error) {
	tx := &types.Transaction{}
	err := rlp.DecodeBytes(txHex, tx)
	if err != nil {
		return "", fmt.Errorf("Decode error: %s", err)
	}
	err = this.ethClient.SendTransaction(context.Background(), tx)
	if err != nil {
		return "", fmt.Errorf("newTx: send tx %s failed, err: %s", tx.Hash().String(), err)
	}
	return tx.Hash().String(), nil
}

func (this *EthManager) VerifyTx(txHash string) bool {
	hash := common.HexToHash(txHash)
	retry := 0
	for {
		receipt, err := this.ethClient.TransactionReceipt(context.Background(), hash)
		if receipt != nil && receipt.Status == types.ReceiptStatusSuccessful {
			return true
		}

		if err != nil && retry < config.EthRetryLimit {
			retry += 1
			time.Sleep(time.Duration(retry*config.EthSleepTime) * time.Second)
			continue
		}

		if err != nil && retry >= config.EthRetryLimit {
			log.Errorf("retry: %d,TransactionReceipt error: %s", retry, err)
			return false
		}
		if receipt != nil && receipt.Status == types.ReceiptStatusSuccessful {
			return true
		} else {
			bs, err := receipt.MarshalJSON()
			if err != nil {
				log.Errorf("MarshalJSON error: %s", err)
			} else {
				log.Errorf("verify tx failed, err: %s", string(bs))
			}
			return false
		}
	}
}
