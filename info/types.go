package info

import (
	"strings"
)

type Serverinfo struct {
	name     string
	father   string
	children []string
}

func NewServerInfo(
	n string,
	f string,
	c []string,
) Serverinfo {
	return Serverinfo{
		name:     n,
		father:   f,
		children: c,
	}
}

func GetServerInfoName(infoptr *Serverinfo) string {
	return infoptr.name
}
func GetServerInfoFather(infoptr *Serverinfo) string {
	return infoptr.father
}
func GetServerInfoChildren(infoptr *Serverinfo) []string {
	return infoptr.children
}

func StringifyInfo(infoptr *Serverinfo) string {
	var sb strings.Builder

	sb.WriteString("Name: " + infoptr.name + "\n")
	sb.WriteString("Father: " + infoptr.father + "\n")
	sb.WriteString("Children: ")

	for _, str := range infoptr.children {
		sb.WriteString(str)
		sb.WriteString(" ")
	}
	sb.WriteString("\n")

	return sb.String()
}
