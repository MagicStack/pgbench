.PHONY: build clean

build:
	$(MAKE) -C _golang
	$(MAKE) -C _nodejs
	$(MAKE) -C _python

clean:
	$(MAKE) -C _golang clean
	$(MAKE) -C _nodejs clean
	$(MAKE) -C _python clean
