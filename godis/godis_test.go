package godis_test

import (
	"common118/godis"
	"fmt"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	gString := godis.NewGodis[string]()
	gString.Set("a", "ssss")
	fmt.Println(gString.Get("a"))
	fmt.Println(gString.Get("b"))

	glist := godis.NewGodis[[]string]()
	glist.Set("a", []string{"ssss", "ssss"})
	fmt.Println(glist.Get("a"))
	fmt.Println(glist.Get("b"))
	for i := 0; i < 9999; i++ {
		go gString.Set("a", "ssss")
		go gString.Get("a")
	}
	time.Sleep(time.Second * 8)
}
