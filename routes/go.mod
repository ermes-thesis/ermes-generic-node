module routes

go 1.24.2

replace clientrepo => ../clientrepo

replace info => ../info

replace helper => ../helper

require (
	clientrepo v0.0.0-00010101000000-000000000000
	helper v0.0.0-00010101000000-000000000000
	info v0.0.0-00010101000000-000000000000
)
