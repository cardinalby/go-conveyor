// Separate module: the benchmark pulls in gonum/plot (and its image/font deps),
// which the main service must not depend on. It reaches the conveyor package via
// the replace directive below.
module github.com/cardinalby/go-conveyor/bench

go 1.26.3

require (
	github.com/cardinalby/go-conveyor v0.0.0
	github.com/stretchr/testify v1.11.1
	gonum.org/v1/plot v0.17.0
)

require (
	codeberg.org/go-fonts/liberation v0.5.0 // indirect
	codeberg.org/go-latex/latex v0.2.0 // indirect
	codeberg.org/go-pdf/fpdf v0.11.1 // indirect
	git.sr.ht/~sbinet/gg v0.7.0 // indirect
	github.com/ajstarks/svgo v0.0.0-20211024235047-1546f124cd8b // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	golang.org/x/image v0.30.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/cardinalby/go-conveyor => ../
