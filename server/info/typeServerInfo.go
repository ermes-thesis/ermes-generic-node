// Package info contains the main information stored by each node.
// This info is readonly and is defined only when launching the application.
package info

import (
	"config"
	"fmt"
	"strconv"
	"strings"
)

// A ServerInfo contains the main attributes of the Server instance.
//
// Every field of the struct should be treated as a constant value,
// hence after creation it should not be modified by any goroutine.
type ServerInfo struct {
	Node
	// Father contains the address where the father is located (as a string).
	//
	// For the moment it contains only the port,
	// hence it can be localized as 'http://localhost:<Father>'.
	//
	// If no father is set for the current node, a special 'nil' value should be used.
	Father *Node
	// Children contains an array of string literals, one per child,
	// whom identify uniquely the name of each child of the current Ermes node.
	//
	// Again as the father each child can be localized as 'http://localhost:<Children[i]>',
	//
	// If the node is a leaf, a special 'nil' value should be used.
	Children []Node
	// The secret is the private header value with which the ermes nodes can communicate.
	// For privacy reasons it must not be exposed to the user.
	Secret           string
	MiddlewareSecret string

	CloudNode *Node

	// Address:port of the Group Token Provider
	GroupTokenProvider string
}

type Node struct {
	Address        string
	MetadataDomain string
	Latency        uint64
}

func NewServerInfo(config config.Config) (*ServerInfo, error) {

	// Parse info about the name, parent and children
	// TODO: check if convention on names is respected (which we do not have yet)

	// NB: these operations just copy the references !
	// TODO: careful for the moment we do not manage errors in the configuration
	name := config.Node.Port
	domain := config.Node.Domain
	parent := config.Hierarchy.Father[0]
	parentDomain := config.Hierarchy.Father[1]
	parentLatency, _ := strconv.ParseUint(config.Hierarchy.Father[2], 10, 64)
	cloudNode := config.Hierarchy.Cloud[0]
	cloudNodeDomain := config.Hierarchy.Cloud[1]
	childrenArgs := config.Hierarchy.Children

	children := make([]Node, len(childrenArgs)/3)
	for i := 0; i < len(children); i++ {
		childLatency, _ := strconv.ParseUint(childrenArgs[3*i+2], 10, 64)
		children[i] = Node{
			Address:        childrenArgs[3*i],
			MetadataDomain: childrenArgs[3*i+1],
			Latency:        childLatency,
		}
	}

	serverinfo := &ServerInfo{
		Secret:           config.Node.Secret,
		MiddlewareSecret: config.Node.MiddlewareSecret,
		CloudNode: &Node{
			Address:        cloudNode,
			MetadataDomain: cloudNodeDomain,
		},
		Father: &Node{
			Address:        parent,
			MetadataDomain: parentDomain,
			Latency:        parentLatency,
		},
		Children: children,
		Node: Node{
			Address:        name,
			MetadataDomain: domain,
		},
		GroupTokenProvider: config.Scheduler.GroupTokenProviderAddress + ":" + config.Scheduler.GroupTokenProviderPort,
	}

	return serverinfo, nil
}

// StringifyInfo returns a pretty-formatted string of the input ServerInfo.
func StringifyInfo(infoptr *ServerInfo) string {
	var sb strings.Builder

	sb.WriteString("Name: " + infoptr.Address + "\n")
	sb.WriteString("Domain: " + infoptr.MetadataDomain + "\n")
	sb.WriteString("Father: " + infoptr.Father.Address + "\n")
	sb.WriteString("FatherDomain: " + infoptr.Father.MetadataDomain + "\n")
	sb.WriteString("FatherLatency: " + fmt.Sprint(infoptr.Father.Latency) + "\n")
	sb.WriteString("Children: ")

	for _, str := range infoptr.Children {
		sb.WriteString(str.Address)
		sb.WriteString(" ")
		sb.WriteString(str.MetadataDomain)
		sb.WriteString(" ")
		sb.WriteString(fmt.Sprint(str.Latency))
		sb.WriteString(" | ")
	}
	sb.WriteString("\n")

	return sb.String()
}
