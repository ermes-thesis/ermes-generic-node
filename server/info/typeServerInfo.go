// Package info contains the main information stored by each node.
// This info is readonly and is defined only when launching the application.
package info

import (
	"errors"
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
	Secret string

	CloudNode *Node
}

type Node struct {
	Address        string
	MetadataDomain string
}

func NewServerInfo(args []string) (*ServerInfo, error) {
	// Extract the information of port, name, parent, children in this order
	if len(args) < 6 {
		return nil, errors.New("too few arguments in the program, cannot launch correctly")
	}

	// Parse first argument
	port, err := strconv.Atoi(args[1])

	if err != nil || port < 1 {
		return nil, errors.New("first argument of the program must be a non-negative integer")
	}

	// Parse info about the name, parent and children
	// TODO: check if convention on names is respected (which we do not have yet)

	// NB: these operations just copy the references !
	name := args[2]
	domain := args[3]
	parent := args[4]
	parentDomain := args[5]
	cloudNode := args[6]
	cloudNodeDomain := args[7]
	childrenArgs := args[8:]

	children := make([]Node, len(childrenArgs)/2)
	for i := 0; i < len(children)/2; i++ {
		children[i] = Node{
			Address:        childrenArgs[2*i],
			MetadataDomain: childrenArgs[2*i+1],
		}
	}

	serverinfo := &ServerInfo{
		Secret: "ermes-secret", // For the moment leave it hardcoded
		CloudNode: &Node{
			Address:        cloudNode,
			MetadataDomain: cloudNodeDomain,
		},
		Father: &Node{
			Address:        parent,
			MetadataDomain: parentDomain,
		},
		Children: children,
		Node: Node{
			Address:        name,
			MetadataDomain: domain,
		},
	}

	return serverinfo, nil
}

// StringifyInfo returns a pretty-formatted string of the input ServerInfo.
func StringifyInfo(infoptr *ServerInfo) string {
	var sb strings.Builder

	sb.WriteString("Name: " + infoptr.Address + "\n")
	sb.WriteString("Father: " + infoptr.Father.Address + "\n")
	sb.WriteString("Children: ")

	for _, str := range infoptr.Children {
		sb.WriteString(str.Address)
		sb.WriteString(" ")
	}
	sb.WriteString("\n")

	return sb.String()
}
