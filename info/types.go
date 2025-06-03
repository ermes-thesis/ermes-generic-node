// Package info contains the main information stored by each node.
// This info is readonly and is defined only when launching the application.
package info

import (
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
