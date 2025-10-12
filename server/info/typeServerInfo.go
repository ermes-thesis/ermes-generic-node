// Package info contains the main information stored by each node.
// This info is readonly and is defined only when launching the application.
package info

import (
	"config"
	"fmt"
	"strings"
)

// A ServerInfo contains the main attributes of the Server instance.
//
// Every field of the struct should be treated as a constant value,
// hence after creation it should not be modified by any goroutine.
type ServerInfo struct {
	HostName       string
	Address        string
	Port           string
	MetadataDomain string
	// Father contains the address where the father is located (as a string).
	//
	// If no father is set for the current node, a special 'nil' value should be used.
	Father *Node
	// Children contains an array of string literals, one per child,
	// whom identify uniquely the name of each child of the current Ermes node.
	//
	// If the node is a leaf, a special 'nil' value should be used.
	Children []Node
	// The secret is the private header value with which the ermes nodes can communicate.
	// For privacy reasons it must not be exposed to the user.
	Secret       string
	ClientSecret string

	CloudNode *Node

	// Address:port of the Group Token Provider
	GroupTokenProviderHostName string
}

type Node struct {
	HostName       string
	MetadataDomain string
	Latency        uint64
}

func NewServerInfo(config config.Config) (*ServerInfo, error) {

	// Parse info about the name, parent and children
	// TODO: check if convention on names is respected (which we do not have yet)

	// NB: these operations just copy the references !
	// TODO: careful for the moment we do not manage errors in the configuration
	hostname := config.Node.Name
	address := config.Node.Address
	port := config.Node.Port // All the processes are open on the same port
	domain := config.Node.Domain
	var father *Node = nil
	var cloudNode *Node = nil
	if config.Hierarchy.Father != nil {
		father = &Node{
			HostName:       config.Hierarchy.Father.Address,
			MetadataDomain: config.Hierarchy.Father.Domain,
			Latency:        config.Hierarchy.Father.LatencyUp,
		}
	}
	if config.Hierarchy.Cloud != nil {
		cloudNode = &Node{
			HostName:       config.Hierarchy.Cloud.Address,
			MetadataDomain: config.Hierarchy.Cloud.Domain,
			Latency:        config.Hierarchy.Cloud.LatencyUp,
		}
	}

	children := make([]Node, len(config.Hierarchy.Children))
	for i, child := range config.Hierarchy.Children {
		children[i] = Node{
			HostName:       child.Address,
			MetadataDomain: child.Domain,
			Latency:        child.LatencyUp,
		}
	}

	serverInfo := &ServerInfo{
		HostName:                   hostname,
		Address:                    address,
		Port:                       port,
		MetadataDomain:             domain,
		Secret:                     config.Node.Secret,
		ClientSecret:               config.Node.ClientSecret,
		CloudNode:                  cloudNode,
		Father:                     father,
		Children:                   children,
		GroupTokenProviderHostName: config.Scheduler.GroupTokenProviderAddress + ":" + config.Scheduler.GroupTokenProviderPort,
	}

	return serverInfo, nil
}

// StringifyInfo returns a pretty-formatted string of the input ServerInfo.
func StringifyInfo(infoptr *ServerInfo) string {
	var sb strings.Builder

	sb.WriteString("HostName: " + infoptr.HostName + "\n")
	sb.WriteString("Domain: " + infoptr.MetadataDomain + "\n")
	if infoptr.Father != nil {
		sb.WriteString("Father: " + infoptr.Father.HostName + "\n")
		sb.WriteString("FatherDomain: " + infoptr.Father.MetadataDomain + "\n")
		sb.WriteString("FatherLatency: " + fmt.Sprint(infoptr.Father.Latency) + "\n")
	}
	sb.WriteString("Children: ")

	for _, str := range infoptr.Children {
		sb.WriteString(str.HostName)
		sb.WriteString(" ")
		sb.WriteString(str.MetadataDomain)
		sb.WriteString(" ")
		sb.WriteString(fmt.Sprint(str.Latency))
		sb.WriteString(" | ")
	}
	sb.WriteString("\n")

	return sb.String()
}
