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
	Name           string
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
	Secret string

	CloudNode *Node
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
	name := config.Node.Name
	port := config.Node.Port // All the processes are open on the same port
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
			HostName:       childrenArgs[3*i],
			MetadataDomain: childrenArgs[3*i+1],
			Latency:        childLatency,
		}
	}

	serverinfo := &ServerInfo{
		Name:           name,
		Port:           port,
		MetadataDomain: domain,
		Secret:         config.Node.Secret, // For the moment leave it hardcoded
		CloudNode: &Node{
			HostName:       cloudNode,
			MetadataDomain: cloudNodeDomain,
		},
		Father: &Node{
			HostName:       parent,
			MetadataDomain: parentDomain,
			Latency:        parentLatency,
		},
		Children: children,
	}

	return serverinfo, nil
}

// StringifyInfo returns a pretty-formatted string of the input ServerInfo.
func StringifyInfo(infoptr *ServerInfo) string {
	var sb strings.Builder

	sb.WriteString("Name: " + infoptr.Name + "\n")
	sb.WriteString("Port: " + infoptr.Port + "\n")
	sb.WriteString("Domain: " + infoptr.MetadataDomain + "\n")
	sb.WriteString("Father: " + infoptr.Father.HostName + "\n")
	sb.WriteString("FatherDomain: " + infoptr.Father.MetadataDomain + "\n")
	sb.WriteString("FatherLatency: " + fmt.Sprint(infoptr.Father.Latency) + "\n")
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
