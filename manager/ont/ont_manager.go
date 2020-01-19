package ont

import (
	"fmt"
	"github.com/CandyDrop/utils"
	"github.com/ontio/bonus/config"
	"github.com/ontio/bonus/manager/transfer"
	sdk "github.com/ontio/ontology-go-sdk"
	"github.com/ontio/ontology/common"
	"github.com/ontio/ontology/common/log"
	"github.com/ontio/ontology/core/types"
	"github.com/ontio/ontology/smartcontract/service/native/ont"
	"math/big"
	"os"
	"time"
	common2 "github.com/ontio/bonus/common"
)

var OntIDVersion = byte(0)

var ontRpcIpAddress = []string{"http://dappnode1.ont.io:20336", "http://dappnode2.ont.io:20336",
	"http://dappnode3.ont.io:20336"}

type OntManager struct {
	account             *sdk.Account
	ontSdk              *sdk.OntologySdk
	oep4ContractAddress common.Address
	cfg                 *config.Ont
	ipIndex             int
	precision           int
	txHandleTask        *transfer.TxHandleTask
	eatp                *common2.ExcelAndTransferParam
}

func NewOntManager(cfg *config.Ont, eatp *common2.ExcelAndTransferParam) (*OntManager, error) {
	ontSdk := sdk.NewOntologySdk()
	ontSdk.NewRpcClient().SetAddress(cfg.OntJsonRpcAddress)

	if cfg.WalletFile == "" {
		cfg.WalletFile = fmt.Sprintf("%s%s%s", config.DefaultWalletPath, string(os.PathSeparator), "ont")
	}
	err := common2.CheckPath(cfg.WalletFile)
	if err != nil {
		return nil, err
	}

	walletName := fmt.Sprintf("%s%s.dat", "ont_", eatp.FileName)
	walletFile := fmt.Sprintf("%s%s%s", cfg.WalletFile, string(os.PathSeparator), walletName)
	var wallet *sdk.Wallet
	if !common2.PathExists(walletFile) {
		wallet, err = ontSdk.CreateWallet(walletFile)
		if err != nil {
			return nil, err
		}
	}  else {
		wallet, err = ontSdk.OpenWallet(walletFile)
		if err != nil {
			log.Fatalf("Can't open local wallet: %s", err)
			return nil, fmt.Errorf("password is wrong")
		}
	}

	log.Infof("ont walletFile: %s", walletFile)
	acc, err := wallet.GetDefaultAccount([]byte(config.PASSWORD))
	if err != nil {
		return nil, err
	}
	log.Infof("ont admin address: %s", acc.Address.ToBase58())

	ontManager := &OntManager {
		account: acc,
		ontSdk:  ontSdk,
		cfg:     cfg,
		eatp:    eatp,
	}

	return ontManager, nil
}

func (self *OntManager) VerifyAddress(address string) error {
	_, err := common.AddressFromBase58(address)
	return err
}

func (self *OntManager) InertSql() {

}
func (self *OntManager) StartTransfer() {
	self.StartHandleTxTask()
	for _, trParam := range self.eatp.BillList {
		self.txHandleTask.TransferQueue <- trParam
	}
}

func (self *OntManager) StartHandleTxTask() {
	txHandleTask := transfer.NewTxHandleTask()
	self.txHandleTask = txHandleTask
	go self.txHandleTask.StartHandleTransferTask(self, self.eatp.FileName)
	go self.txHandleTask.StartVerifyTxTask(self)
}

func (self *OntManager) WithdrawToken(address string) error {
	_, txHex, err := self.NewWithdrawTx(address, "")
	if err != nil {
		log.Errorf("NewWithdrawTx failed, error: %s", err)
		return fmt.Errorf("NewWithdrawTx failed, error: %s", err)
	}
	hash, err := self.SendTx(txHex)
	if err != nil {
		log.Errorf("SendTx failed,txhash: %s, error: %s", hash, err)
		return fmt.Errorf("SendTx failed,txhash: %s, error: %s", hash, err)
	}
	boo := self.VerifyTx(hash)
	if !boo {
		log.Errorf("VerifyTx failed,txhash: %s, error: %s", hash, err)
		return fmt.Errorf("VerifyTx failed,txhash: %s, error: %s", hash, err)
	}
	return nil
}

func (self *OntManager) SetContractAddress(address string) error {
	addr, err := common.AddressFromHexString(address)
	if err != nil {
		return err
	}
	self.oep4ContractAddress = addr
	//update precision
	preResult, err := self.ontSdk.NeoVM.PreExecInvokeNeoVMContract(addr,
		[]interface{}{"decimals", []interface{}{}})
	if err != nil {
		return err
	}
	res, err := preResult.Result.ToInteger()
	if err != nil {
		return err
	}
	self.precision = int(res.Int64())
	return nil
}

func (self *OntManager) NewWithdrawTx(destAddr string, amount string) (string, []byte, error) {
	address, err := common.AddressFromBase58(destAddr)
	if err != nil {
		return "", nil, fmt.Errorf("common.AddressFromBase58 error: %s", err)
	}
	var tx *types.MutableTransaction
	if self.eatp.TokenType == config.ONT {
		value := utils.ParseAssetAmount(amount, config.ONT_DECIMALS)
		var sts []ont.State
		sts = append(sts, ont.State{
			From:  self.account.Address,
			To:    address,
			Value: value,
		})
		params := ont.Transfers{
			States: sts,
		}
		tx, err = self.ontSdk.Native.NewNativeInvokeTransaction(self.cfg.GasPrice, self.cfg.GasLimit,
			OntIDVersion, sdk.ONT_CONTRACT_ADDRESS, "transfer", []interface{}{params})
		if err != nil {
			return "", nil, fmt.Errorf("transfer ont, this.ontologySdk.Native.NewNativeInvokeTransaction error: %s", err)
		}
		err = self.ontSdk.SignToTransaction(tx, self.account)
		if err != nil {
			return "", nil, fmt.Errorf("transfer ont: this.ontologySdk.SignToTransaction err: %s", err)
		}
	} else if self.eatp.TokenType == config.ONG {
		value := utils.ParseAssetAmount(amount, config.ONG_DECIMALS)
		var sts []ont.State
		sts = append(sts, ont.State{
			From:  self.account.Address,
			To:    address,
			Value: value,
		})
		params := ont.Transfers{
			States: sts,
		}
		tx, err = self.ontSdk.Native.NewNativeInvokeTransaction(self.cfg.GasPrice, self.cfg.GasLimit,
			OntIDVersion, sdk.ONG_CONTRACT_ADDRESS, "transfer", []interface{}{params})
		if err != nil {
			return "", nil, fmt.Errorf("transfer ong, this.ontologySdk.Native.NewNativeInvokeTransaction error: %s", err)
		}
		err = self.ontSdk.SignToTransaction(tx, self.account)
		if err != nil {
			return "", nil, fmt.Errorf("transfer ong, this.ontologySdk.SignToTransaction err: %s", err)
		}
	} else {
		if self.oep4ContractAddress == common.ADDRESS_EMPTY {
			return "", nil, fmt.Errorf("oep4ContractAddress is nil")
		}
		val := utils.ParseAssetAmount(amount, self.precision)
		value := new(big.Int).SetUint64(val)
		tx, err = self.ontSdk.NeoVM.NewNeoVMInvokeTransaction(self.cfg.GasPrice, self.cfg.GasLimit, self.oep4ContractAddress, []interface{}{"transfer", []interface{}{self.account.Address, address, value}})
		if err != nil {
			return "", nil, fmt.Errorf("NewNeoVMInvokeTransaction error: %s", err)
		}
		err = self.ontSdk.SignToTransaction(tx, self.account)
		if err != nil {
			return "", nil, fmt.Errorf("OEP4 SignToTransaction error: %s", err)
		}
	}
	t, err := tx.IntoImmutable()
	if err != nil {
		return "", nil, fmt.Errorf("IntoImmutable error: %s", err)
	}
	h := tx.Hash()
	return h.ToHexString(), t.ToArray(), nil
}

func (self *OntManager) GetAdminAddress() string {
	return self.account.Address.ToBase58()
}

func (self *OntManager) GetTxTime(txHash string) (uint32, error) {
	height, err := self.ontSdk.GetBlockHeightByTxHash(txHash)
	if err != nil {
		return 0, err
	}
	block, err := self.ontSdk.GetBlockByHeight(height)
	if err != nil {
		return 0, err
	}
	return block.Header.Timestamp, nil
}
func (self *OntManager) SendTx(txHex []byte) (string, error) {
	tx, err := types.TransactionFromRawBytes(txHex)
	if err != nil {
		return "", fmt.Errorf("TransactionFromRawBytes error: %s", err)
	}
	txMu, err := tx.IntoMutable()
	if err != nil {
		return "", fmt.Errorf("IntoMutable error: %s", err)
	}
	txHash, err := self.ontSdk.SendTransaction(txMu)
	if err != nil {
		return "", fmt.Errorf("SendTransaction error: %s", err)
	}
	return txHash.ToHexString(), nil
}
func (self *OntManager) changeIpAddress() {
	index := (self.ipIndex + 1) % len(ontRpcIpAddress)
	self.ontSdk.NewRpcClient().SetAddress(ontRpcIpAddress[index])
}

func (self *OntManager) VerifyTx(txHash string) bool {
	retry := 0
	for {
		event, err := self.ontSdk.GetSmartContractEvent(txHash)
		if event != nil && event.State == 0 {
			return false
		}
		if err != nil && retry < config.RetryLimit {
			retry += 1
			time.Sleep(time.Duration(retry*config.SleepTime) * time.Second)
			continue
		}
		if err != nil && retry >= config.RetryLimit {
			log.Errorf("GetSmartContractEvent fail, txhash: %s, err: %s", txHash, err)
			return false
		}
		if event == nil && retry < config.RetryLimit {
			retry += 1
			time.Sleep(time.Duration(retry*config.SleepTime) * time.Second)
			continue
		}
		if event == nil {
			return false
		}
		if event.State == 1 {
			if self.oep4ContractAddress != common.ADDRESS_EMPTY {
				if len(event.Notify) == 2 {
					return true
				} else {
					return false
				}
			}
			return true
		}
		return false
	}
}
