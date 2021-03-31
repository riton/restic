package filecoin

import (
	"fmt"
	"net/http"
	"strings"

	httpapi "github.com/ipfs/go-ipfs-http-client"
	"github.com/multiformats/go-multiaddr"
)

func newDecoratedIPFSAPI(proxyAddr, ffsToken string) (*httpapi.HttpApi, error) {
	ipport := strings.Split(proxyAddr, ":")
	if len(ipport) != 2 {
		return nil, fmt.Errorf("ipfs addr is invalid")
	}
	cm, err := multiaddr.NewComponent("dns4", ipport[0])
	if err != nil {
		return nil, err
	}
	cp, err := multiaddr.NewComponent("tcp", ipport[1])
	if err != nil {
		return nil, err
	}
	useHTTPS := ipport[1] == "443"
	ipfsMaddr := cm.Encapsulate(cp)
	customClient := http.DefaultClient
	customClient.Transport = newFFSHeaderDecorator(ffsToken, useHTTPS)
	ipfs, err := httpapi.NewApiWithClient(ipfsMaddr, customClient)
	if err != nil {
		return nil, err
	}
	return ipfs, nil
}

type ffsHeaderDecorator struct {
	ffsToken string
	useHTTPS bool
}

func newFFSHeaderDecorator(ffsToken string, useHTTPS bool) *ffsHeaderDecorator {
	return &ffsHeaderDecorator{
		ffsToken: ffsToken,
		useHTTPS: useHTTPS,
	}
}

func (fhd ffsHeaderDecorator) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header["x-ipfs-ffs-auth"] = []string{fhd.ffsToken}
	if fhd.useHTTPS {
		req.URL.Scheme = "https"
	}

	return http.DefaultTransport.RoundTrip(req)
}
