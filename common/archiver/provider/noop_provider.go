package provider

import (
	"context"

	"github.com/uber/cadence/common/archiver"
)

type noOpArchiverProvider struct{}

func NewNoOpArchiverProvider() ArchiverProvider {
	return &noOpArchiverProvider{}
}

func (*noOpArchiverProvider) RegisterBootstrapContainer(
	serviceName string,
	historyContainer *archiver.HistoryBootstrapContainer,
	visibilityContainter *archiver.VisibilityBootstrapContainer,
) error {
	return nil
}

func (*noOpArchiverProvider) GetHistoryArchiver(scheme, serviceName string) (archiver.HistoryArchiver, error) {
	return &noOpHistoryArchiver{}, nil
}

func (*noOpArchiverProvider) GetVisibilityArchiver(scheme, serviceName string) (archiver.VisibilityArchiver, error) {
	return &noOpVisibilityArchiver{}, nil
}

type noOpHistoryArchiver struct{}

func (*noOpHistoryArchiver) Archive(context.Context, archiver.URI, *archiver.ArchiveHistoryRequest, ...archiver.ArchiveOption) error {
	return nil
}

func (*noOpHistoryArchiver) Get(context.Context, archiver.URI, *archiver.GetHistoryRequest) (*archiver.GetHistoryResponse, error) {
	return &archiver.GetHistoryResponse{}, nil
}

func (*noOpHistoryArchiver) ValidateURI(archiver.URI) error {
	return nil
}

type noOpVisibilityArchiver struct{}

func (*noOpVisibilityArchiver) Archive(context.Context, archiver.URI, *archiver.ArchiveVisibilityRequest, ...archiver.ArchiveOption) error {
	return nil
}

func (*noOpVisibilityArchiver) Query(context.Context, archiver.URI, *archiver.QueryVisibilityRequest) (*archiver.QueryVisibilityResponse, error) {
	return &archiver.QueryVisibilityResponse{}, nil
}

func (*noOpVisibilityArchiver) ValidateURI(archiver.URI) error {
	return nil
}
