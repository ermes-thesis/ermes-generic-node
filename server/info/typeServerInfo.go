// Package info contains the main information stored by each node.
// This info is readonly and is defined only when launching the application.
package info

import (
	"errors"
	"strconv"
	"strings"
)

// A Serverinfo contains the main attributes of the Server instance.
//
// Every field of the struct should be treated as a constant value,
// hence after creation it should not be modified by any goroutine.
type Serverinfo struct {
	// Name contains the string literal which describes
	// uniquely an Ermes node in the network.
	//
	// It might be useful for the clients who connect to the node,
	Name string
	// Father contains the address where the father is located (as a string).
	//
	// For the moment it contains only the port,
	// hence it can be localized as 'http://localhost:<Father>'.
	//
	// If no father is set for the current node, a special 'nil' value should be used.
	Father string
	// Children contains an array of string literals, one per child,
	// whom identify uniquely the name of each child of the current Ermes node.
	//
	// Again as the father each child can be localized as 'http://localhost:<Children[i]>',
	//
	// If the node is a leaf, a special 'nil' value should be used.
	Children []string
	// The secret is the private header value with which the ermes nodes can communicate.
	// For privacy reasons it must not be exposed to the user.
	Secret string

	MetadataDomain string

	CloudNode string
}

func NewServerinfo(args []string) (*Serverinfo, error) {
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
	parent := args[3]
	domain := args[4]
	cloudNode := args[5]
	children := args[6:]

	serverinfo := &Serverinfo{
		Name:           name,
		Father:         parent,
		Children:       children,
		Secret:         "ermes-secret", // For the moment leave it hardcoded
		MetadataDomain: domain,
		CloudNode:      cloudNode,
	}

	return serverinfo, nil
}

// StringifyInfo returns a pretty-formatted string of the input Serverinfo.
func StringifyInfo(infoptr *Serverinfo) string {
	var sb strings.Builder

	sb.WriteString("Name: " + infoptr.Name + "\n")
	sb.WriteString("Father: " + infoptr.Father + "\n")
	sb.WriteString("Children: ")

	for _, str := range infoptr.Children {
		sb.WriteString(str)
		sb.WriteString(" ")
	}
	sb.WriteString("\n")

	return sb.String()
}
