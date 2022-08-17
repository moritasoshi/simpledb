.PHONY: test fmt

GO         = go
GO_TEST    = $(GO) test -v
GO_FORMAT  = $(GO) fmt
GO_PKGROOT = ./...

test:
	$(GO_TEST) $(GO_PKGROOT)
fmt:
	$(GO_FORMAT) $(GO_PKGROOT)
