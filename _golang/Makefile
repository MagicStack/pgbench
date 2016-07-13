.PHONY: clean


pgbench_golang:
	rm -rf "$(CURDIR)/_go" && mkdir "$(CURDIR)/_go"
	GOPATH="$(CURDIR)/_go" go get gopkg.in/alecthomas/kingpin.v2 \
								  github.com/lib/pq \
								  github.com/jackc/pgx
	mkdir -p "$(CURDIR)/_go/src/github.com/magicstack/pgbench/"
	ln -s "$(CURDIR)" "$(CURDIR)/_go/src/github.com/magicstack/pgbench/_golang"
	GOPATH="$(CURDIR)/_go" go build -o pgbench_golang github.com/magicstack/pgbench/_golang
	rm -rf "$(CURDIR)/_go"

clean:
	rm -f "$(CURDIR)/pgbench_golang"
	rm -rf "$(CURDIR)/_go"
