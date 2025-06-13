module main

go 1.24.2

replace routes => ../routes

replace info => ../info

replace clientrepo => ../clientrepo

require (
	clientrepo v0.0.0-00010101000000-000000000000
	info v0.0.0-00010101000000-000000000000
	routes v0.0.0-00010101000000-000000000000
)
