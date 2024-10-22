package mevshare

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/flashbots/mev-share-node/metrics"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"

	"gopkg.in/yaml.v3"
)

var ErrInvalidBuilder = errors.New("invalid builder specification")

type BuilderAPI uint8

const (
	BuilderAPIRefundRecipient BuilderAPI = iota
	BuilderAPIMevShareBeta1
	BuilderAPIMevShareBeta1Replacement

	OrderflowHeaderName = "x-orderflow-origin"
)

func parseBuilderAPI(api string) (BuilderAPI, error) {
	switch api {
	case "refund-recipient":
		return BuilderAPIRefundRecipient, nil
	case "v0.1":
		return BuilderAPIMevShareBeta1, nil
	case "v0.1-auth":
		return BuilderAPIMevShareBeta1Replacement, nil
	default:
		return 0, ErrInvalidBuilder
	}
}

type BuildersConfig struct {
	Builders []struct {
		Name     string `yaml:"name"`
		URL      string `yaml:"url"`
		API      string `yaml:"api"`
		Internal bool   `yaml:"internal,omitempty"`
		Disabled bool   `yaml:"disabled,omitempty"`
		Delay    bool   `yaml:"delay,omitempty"`
	} `yaml:"builders"`
	OrderflowHeader      bool   `yaml:"orderflowHeader,omitempty"`
	OrderflowHeaderValue string `yaml:"orderflowHeaderValue,omitempty"`
	RestrictedAddress    string `yaml:"restrictedAddress"`
}

// LoadBuilderConfig parses a builder config from a file
func LoadBuilderConfig(file string) (BuildersBackend, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return BuildersBackend{}, err
	}

	var config BuildersConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return BuildersBackend{}, err
	}

	customHeaders := make(map[string]string)
	if config.OrderflowHeader {
		customHeaders[OrderflowHeaderName] = config.OrderflowHeaderValue
	}

	externalBuilders := make([]JSONRPCBuilderBackend, 0)
	internalBuilders := make([]JSONRPCBuilderBackend, 0)
	for _, builder := range config.Builders {
		if builder.Disabled {
			continue
		}

		api, err := parseBuilderAPI(builder.API)
		if err != nil {
			return BuildersBackend{}, err
		}

		cl, err := rpc.DialContext(context.Background(), builder.URL)
		if err != nil {
			return BuildersBackend{}, err
		}

		builderBackend := JSONRPCBuilderBackend{
			Name:   strings.ToLower(builder.Name),
			Client: cl,
			API:    api,
			Delay:  builder.Delay,
		}

		if builder.Internal {
			internalBuilders = append(internalBuilders, builderBackend)
		} else {
			externalBuilders = append(externalBuilders, builderBackend)
		}
	}

	externalBuilderMap := make(map[string]JSONRPCBuilderBackend)
	for _, builder := range externalBuilders {
		externalBuilderMap[builder.Name] = builder
	}

	return BuildersBackend{
		externalBuilders:  externalBuilderMap,
		internalBuilders:  internalBuilders,
		RestrictedAddress: config.RestrictedAddress,
	}, nil
}

type JSONRPCBuilderBackend struct {
	Name   string
	Client *rpc.Client
	API    BuilderAPI
	Delay  bool
	Url    string
}

func (b *JSONRPCBuilderBackend) SendBundle(ctx context.Context, bundle *SendBundleArgs) (err error) {
	startAt := time.Now()
	metrics.IncBundleSentToBuilder(b.Name)
	defer func() {
		metrics.RecordBundleSentToBuilderTime(b.Name, time.Since(startAt).Milliseconds())
		if err != nil {
			metrics.IncBundleSentToBuilderFailure(b.Name)
		}
	}()

	switch b.API {
	case BuilderAPIMevShareBeta1:
		var hash common.Hash
		err = b.Client.CallContext(ctx, &hash, "eth_sendBundle", *bundle)
		if err != nil {
			return err
		}
		return nil
	case BuilderAPIMevShareBeta1Replacement:
		_, err = RazorPost(bundle, b.Url)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (b *JSONRPCBuilderBackend) CancelBundleByHash(ctx context.Context, hash common.Hash) error {
	return nil
}

type BuildersBackend struct {
	externalBuilders  map[string]JSONRPCBuilderBackend
	internalBuilders  []JSONRPCBuilderBackend
	RestrictedAddress string
}

// SendBundle sends a bundle to all builders.
// Bundles are sent to all builders in parallel.
func (b *BuildersBackend) SendBundle(ctx context.Context, logger *zap.Logger, bundle *SendMevBundleArgs, sendExternalBuilders bool) { //nolint:gocognit
	builderBundle := SendBundleArgs{
		MaxBlockNumber: uint64(bundle.Inclusion.MaxBlock),
	}

	for _, v := range bundle.Body {
		builderBundle.Txs = append(builderBundle.Txs, *v.Tx)
		if v.CanRevert {
			var tx types.Transaction
			if err := tx.UnmarshalBinary(*v.Tx); err != nil {
				logger.Error("failed to unmarshal transaction", zap.Error(err))
				return
			}
			builderBundle.RevertingTxHashes = append(builderBundle.RevertingTxHashes, tx.Hash())
		}
	}

	var wg sync.WaitGroup

	// always send to internal builders
	internalBuildersSuccess := make([]bool, len(b.internalBuilders))
	for idx, builder := range b.internalBuilders {
		wg.Add(1)
		go func(builder JSONRPCBuilderBackend, idx int) {
			defer wg.Done()

			start := time.Now()
			err := builder.SendBundle(ctx, &builderBundle)
			logger.Info("Sent bundle to internal builder", zap.String("builder", builder.Name), zap.Duration("duration", time.Since(start)))

			if err != nil {
				logger.Warn("Failed to send bundle to internal builder", zap.Error(err), zap.String("builder", builder.Name))
			} else {
				internalBuildersSuccess[idx] = true
			}
		}(builder, idx)
	}

	//如果交易5min还未上链，就向备份builder发送交易
	if sendExternalBuilders {
		for name, builder := range b.externalBuilders {
			wg.Add(1)
			go func(builder JSONRPCBuilderBackend, name string) {
				defer wg.Done()

				start := time.Now()
				err := builder.SendBundle(ctx, &builderBundle)
				logger.Info("Sent bundle to external builder", zap.String("builder", builder.Name), zap.Duration("duration", time.Since(start)))

				if err != nil {
					logger.Warn("Failed to send bundle to external builder", zap.Error(err), zap.String("builder", builder.Name))
				}
			}(builder, name)
		}
	}

	wg.Wait()

	sentToInternal := false
	for _, success := range internalBuildersSuccess {
		if success {
			sentToInternal = true
			break
		}
	}
	if !sentToInternal {
		logger.Error("Failed to send bundle to any of the internal builders")
	}
}

func (b *BuildersBackend) CancelBundleByHash(ctx context.Context, logger log.Logger, hash common.Hash) {
}

type SendBundleArgs struct {
	Txs               []hexutil.Bytes `json:"txs"`
	MaxBlockNumber    uint64          `json:"maxBlockNumber"`
	MinTimestamp      *uint64         `json:"minTimestamp,omitempty"`
	MaxTimestamp      *uint64         `json:"maxTimestamp,omitempty"`
	RevertingTxHashes []common.Hash   `json:"revertingTxHashes,omitempty"`
}

type RazorRpcRequest struct {
	Jsonrpc string           `json:"jsonrpc"`
	Id      string           `json:"id"`
	Method  string           `json:"method"`
	Params  []SendBundleArgs `json:"params"`
}

type Response struct {
	Jsonrpc string `json:"jsonrpc"`
	Id      string `json:"id"`
	Result  string `json:"result"`
}

func RazorPost(arg *SendBundleArgs, url string) (*common.Hash, error) {
	jsonReq := RazorRpcRequest{
		Jsonrpc: "2.0",
		Id:      uuid.NewString(),
		Method:  "eth_sendBundle",
	}
	jsonReq.Params = append(jsonReq.Params, *arg)
	jdata, _ := json.Marshal(jsonReq)
	body := bytes.NewReader(jdata)
	//transCfg := &http.Transport{
	//	TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // disable verify
	//}
	//// Create Http Client
	//client := &http.Client{Transport: transCfg}
	client := &http.Client{}
	req, _ := http.NewRequest("POST", url, body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "qsO5yGyH8JAXPyuyYEhPYveyf9pd9qlqyBsrs4CSRgFlu4jAfLcFukU4FuctlAxiPjFb8dKTldOCNeorGxFIrTSXNsnI7sVH")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("client.Do:", err)
		return nil, err
	}

	defer resp.Body.Close()
	rb, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("ReadAll:", err)
		return nil, err
	}
	var response Response
	if err = json.Unmarshal(rb, &response); err != nil {
		fmt.Println("Unmarshal:", err)
		return nil, err
	}
	hash := common.HexToHash(response.Result)
	return &hash, nil
}
