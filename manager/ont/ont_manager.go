package ont

import (
	"encoding/json"
	"fmt"
	common2 "github.com/ontio/bonus/common"
	"github.com/ontio/bonus/config"
	"github.com/ontio/bonus/manager/transfer"
	"github.com/ontio/bonus/utils"
	sdk "github.com/ontio/ontology-go-sdk"
	"github.com/ontio/ontology-go-sdk/oep4"
	"github.com/ontio/ontology/common"
	"github.com/ontio/ontology/common/log"
	"github.com/ontio/ontology/core/types"
	"github.com/ontio/ontology/smartcontract/service/native/ont"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"time"
)

var OntIDVersion = byte(0)

type OntManager struct {
	account             *sdk.Account
	ontSdk              *sdk.OntologySdk
	oep4ContractAddress common.Address
	cfg                 *config.Ont
	ipIndex             int
	precision           int
	txHandleTask        *transfer.TxHandleTask
	eatp                *common2.ExcelParam
}

func (this *OntManager) GetFromAddress() {
	var pageNumber *int
	this.getFromAddress(pageNumber)
}

func (this *OntManager) getFromAddress(pageNumber *int) (string, error) {
	*pageNumber += 1
	url := fmt.Sprintf("http://explorer.ont.io/v2/addresses/%s/transactions?page_size=10&page_number=%d", "ASLbwuar3ZTbUbLPnCgjGUw2WHhMfvJJtx", *pageNumber)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	res := make(map[string]interface{})
	err = json.Unmarshal(body, &res)
	if err != nil {
		return "", err
	}
	fmt.Println("response Body:", res)
	result := res["result"].([]interface{})
	for _, item := range result {
		rr := item.(map[string]interface{})
		transfers := rr["transfers"].([]interface{})
		for _, tr := range transfers {
			tfs := tr.(map[string]interface{})
			if tfs["to_address"] == this.account.Address.ToBase58() {
				return tfs["from_address"].(string), nil
			}
		}
	}
	return this.getFromAddress(pageNumber)
}

func NewOntManager(cfg *config.Ont, eatp *common2.ExcelParam) (*OntManager, error) {
	ontSdk := sdk.NewOntologySdk()
	ontSdk.NewRpcClient().SetAddress(cfg.OntJsonRpcAddress)

	if cfg.WalletFile == "" {
		cfg.WalletFile = fmt.Sprintf("%s%s%s", config.DefaultWalletPath, string(os.PathSeparator), "ont")
	}
	err := common2.CheckPath(cfg.WalletFile)
	if err != nil {
		return nil, err
	}

	walletName := fmt.Sprintf("%s%s.dat", "ont_", eatp.EventType)
	walletFile := fmt.Sprintf("%s%s%s", cfg.WalletFile, string(os.PathSeparator), walletName)
	var wallet *sdk.Wallet
	if !common2.PathExists(walletFile) {
		wallet, err = ontSdk.CreateWallet(walletFile)
		if err != nil {
			return nil, err
		}
	} else {
		wallet, err = ontSdk.OpenWallet(walletFile)
		if err != nil {
			log.Fatalf("Can't open local wallet: %s", err)
			return nil, fmt.Errorf("password is wrong")
		}
	}

	log.Infof("ont walletFile: %s", walletFile)
	acct, err := wallet.GetDefaultAccount([]byte(config.PASSWORD))
	if (err != nil && err.Error() == "does not set default account") || acct == nil {
		acct, err = wallet.NewDefaultSettingAccount([]byte(config.PASSWORD))
		if err != nil {
			return nil, err
		}
		err = wallet.Save()
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		log.Errorf("GetDefaultAccount failed, error: %s", err)
		return nil, err
	}
	log.Infof("ont admin address: %s", acct.Address.ToBase58())

	ontManager := &OntManager{
		account: acct,
		ontSdk:  ontSdk,
		cfg:     cfg,
		eatp:    eatp,
	}

	return ontManager, nil
}

func (self *OntManager) VerifyAddress(address string) bool {
	_, err := common.AddressFromBase58(address)
	if err != nil {
		log.Errorf("ont VerifyAddress failed, address: %s", address)
		return false
	}
	return true
}

func (self *OntManager) StartTransfer() {
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

func (self *OntManager) GetStatus() common2.TransferStatus {
	if self.txHandleTask == nil {
		log.Info("self.txHandleTask is nil")
		return common2.NotTransfer
	}
	return self.txHandleTask.TransferStatus
}

func (self *OntManager) StartHandleTxTask() {
	txHandleTask := transfer.NewTxHandleTask(self.eatp.TokenType)
	self.txHandleTask = txHandleTask
	log.Infof("init txHandleTask success, transfer status: %d\n", self.txHandleTask.TransferStatus)
	go self.txHandleTask.StartHandleTransferTask(self, self.eatp.EventType)
	go self.txHandleTask.StartVerifyTxTask(self)
}

func (self *OntManager) WithdrawToken() error {
	bal, err := self.GetAdminBalance()
	if err != nil {
		return fmt.Errorf("GetAdminBalance faied, error: %s", err)
	}
	for tokenType, amt := range bal {
		if amt == "" {
			continue
		}
		self.withdrawToken(tokenType, amt)
	}
	return nil
}

func (self *OntManager) withdrawToken(tokenType, bal string) error {
	var amt string
	if tokenType == config.ONT {
		amt = bal
	} else if self.eatp.TokenType == config.ONG {
		value := utils.ParseAssetAmount(bal, config.ONG_DECIMALS)
		fee := utils.ParseAssetAmount("0.01", config.ONG_DECIMALS)
		if value < fee {
			return fmt.Errorf("balance is less than fee, balance: %s, fee:%s", bal, "0.01")
		}
		transferAmt := value - fee
		b := new(big.Int)
		b.SetUint64(transferAmt)
		amt = utils.ToStringByPrecise(b, config.ONG_DECIMALS)
	} else if self.eatp.TokenType == config.OEP4 {
		amt = bal
	} else {
		return fmt.Errorf("not support token type: %s", self.eatp.TokenType)
	}
	_, txHex, err := self.NewWithdrawTx("", amt)
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

func (self *OntManager) NewBatchWithdrawTx(addrAndAmts [][]string) (string, []byte, error) {
	var sts []ont.State
	for _, addrAndAmt := range addrAndAmts {
		to, err := common.AddressFromBase58(addrAndAmt[0])
		if err != nil {
			return "", nil, fmt.Errorf("AddressFromBase58 error: %s", err)
		}
		var val *big.Int
		if self.eatp.TokenType == config.ONT {
			val = utils.ToIntByPrecise(addrAndAmt[1], config.ONT_DECIMALS)
		} else if self.eatp.TokenType == config.ONG {
			val = utils.ToIntByPrecise(addrAndAmt[1], config.ONG_DECIMALS)
		} else {
			log.Errorf("token type not support, tokenType: %s", self.eatp.TokenType)
			return "", nil, fmt.Errorf("not supprt token type: %s", self.eatp.TokenType)
		}
		st := ont.State{
			From:  self.account.Address,
			To:    to,
			Value: val.Uint64(),
		}
		sts = append(sts, st)
	}
	params := ont.Transfers{
		States: sts,
	}
	tx, err := self.ontSdk.Native.NewNativeInvokeTransaction(self.cfg.GasPrice, self.cfg.GasLimit,
		OntIDVersion, sdk.ONG_CONTRACT_ADDRESS, "transfer", []interface{}{params})
	if err != nil {
		return "", nil, fmt.Errorf("transfer ong, this.ontologySdk.Native.NewNativeInvokeTransaction error: %s", err)
	}
	err = self.ontSdk.SignToTransaction(tx, self.account)
	if err != nil {
		return "", nil, fmt.Errorf("transfer ong, this.ontologySdk.SignToTransaction err: %s", err)
	}
	t, err := tx.IntoImmutable()
	if err != nil {
		return "", nil, fmt.Errorf("IntoImmutable error: %s", err)
	}
	h := tx.Hash()
	return h.ToHexString(), t.ToArray(), nil
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

func (self *OntManager) GetAdminBalance() (map[string]string, error) {
	val, err := self.ontSdk.Native.Ont.BalanceOf(self.account.Address)
	if err != nil {
		return nil, err
	}
	ontBa := strconv.FormatUint(val, 10)
	val, err = self.ontSdk.Native.Ong.BalanceOf(self.account.Address)
	if err != nil {
		return nil, err
	}
	r := new(big.Int)
	r.SetUint64(val)
	ongBa := utils.ToStringByPrecise(r, 9)

	res := make(map[string]string)
	res["Ont"] = ontBa
	res["Ong"] = ongBa
	if self.eatp.TokenType == config.OEP4 {
		oep4 := oep4.NewOep4(self.oep4ContractAddress, self.ontSdk)
		val, err := oep4.BalanceOf(self.account.Address)
		if err != nil {
			return nil, err
		}
		ba := utils.ToStringByPrecise(val, uint64(self.precision))
		res[self.eatp.TokenType] = ba
	}
	return res, nil
}

func (self *OntManager) EstimateFee() (string, error) {
	fee := float64(len(self.eatp.BillList)) * 0.01
	return strconv.FormatFloat(fee, 'f', -1, 64), nil
}

func (self *OntManager) ComputeSum() (string, error) {
	sum := uint64(0)
	if self.eatp.TokenType == config.ONT {
		for _, item := range self.eatp.BillList {
			val, err := strconv.ParseUint(item.Amount, 10, 64)
			if err != nil {
				return "", err
			}
			sum += val
		}
		return strconv.FormatUint(sum, 10), nil
	} else if self.eatp.TokenType == config.ONG {
		for _, item := range self.eatp.BillList {
			val := utils.ParseAssetAmount(item.Amount, 9)
			sum += val
		}
		temp := new(big.Int)
		temp.SetUint64(sum)
		return utils.ToStringByPrecise(temp, uint64(9)), nil
	} else if self.eatp.TokenType == config.OEP4 {
		for _, item := range self.eatp.BillList {
			val := utils.ParseAssetAmount(item.Amount, self.precision)
			sum += val
		}
		temp := new(big.Int)
		temp.SetUint64(sum)
		return utils.ToStringByPrecise(temp, uint64(self.precision)), nil
	} else {
		return "", fmt.Errorf("not support token type: %s", self.eatp.TokenType)
	}
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
