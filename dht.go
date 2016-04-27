package main

import (
	"crypto/sha1"
	"math/big"
	"net"
)

type NodeAddress string
type Key string

const FingerSize = sha1.Size * 8

var hashMod = new(big.Int).Exp(big.NewInt(2), big.NewInt(FingerSize), nil)

type Node struct {
	Address     NodeAddress
	Fingertable []NodeAddress
	NextFinger  int
	Predecessor NodeAddress
	Successor   NodeAddress
	Successors  []NodeAddress
	Bucket      map[Key]string
}

func hashString(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

func jump(fingerentry int, port string) *big.Int {
	n := hashString(port)
	two := big.NewInt(2)
	fingerentryminus1 := big.NewInt(int64(fingerentry) - 1)
	jump := new(big.Int).Exp(two, fingerentryminus1, nil)
	sum := new(big.Int).Add(n, jump)

	return new(big.Int).Mod(sum, hashMod)
}
func between(start, elt, end *big.Int, inclusive bool) bool {
	if end.Cmp(start) > 0 {
		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
	} else {
		return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
	}
	panic("impossible")
}

func getLocalAddress() string {
	var localaddress string

	ifaces, err := net.Interfaces()
	if err != nil {
		panic("Failed to find network interfaces")
	}

	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("Failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddress == "" {
		panic("Failed to find non-loopback interface with valid address")
	}

	return localaddress
}
