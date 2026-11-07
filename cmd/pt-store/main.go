// Command pt-store keeps PicoTera's request artifacts readable after old days are packed into
// daily archives: serve answers the dashboard's reads, archive packs, get reads one entry back.
package main

import (
	"os"

	"github.com/orzfly/picotera-tools/internal/store"
)

func main() {
	os.Exit(store.Main(os.Args))
}
