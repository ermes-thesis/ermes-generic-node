//go:build test
// +build test

package childUsage

func (s *childResourceUsage) GetWrite() resourceUsage {
	return s.Write
}

func (s *childResourceUsage) GetRead() resourceUsage {
	return s.Read
}
