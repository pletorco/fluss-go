package fadm

import (
	"context"
	"fmt"
	"strings"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

// ACLResourceType identifies a resource category in the Fluss ACLBinding protocol.
type ACLResourceType int32

// ACLBinding resource types supported by Apache Fluss 1.0.
const (
	ACLResourceAny      ACLResourceType = 1
	ACLResourceCluster  ACLResourceType = 2
	ACLResourceDatabase ACLResourceType = 3
	ACLResourceTable    ACLResourceType = 4
)

// ACLOperation identifies an operation protected by an ACL binding.
type ACLOperation int32

// ACLBinding operations supported by Apache Fluss 1.0.
const (
	ACLOperationAny      ACLOperation = 1
	ACLOperationAll      ACLOperation = 2
	ACLOperationRead     ACLOperation = 3
	ACLOperationWrite    ACLOperation = 4
	ACLOperationCreate   ACLOperation = 5
	ACLOperationDrop     ACLOperation = 6
	ACLOperationAlter    ACLOperation = 7
	ACLOperationDescribe ACLOperation = 8
)

// ACLPermission identifies whether an operation is allowed.
// Apache Fluss 1.0 does not support deny ACLs.
type ACLPermission int32

// ACLBinding permissions supported by Apache Fluss 1.0.
const (
	ACLPermissionAny   ACLPermission = 1
	ACLPermissionAllow ACLPermission = 2
)

// ACLPrincipalType identifies the namespace of an ACLBinding principal.
// Custom authorizers may define additional non-empty, case-sensitive values.
type ACLPrincipalType string

// Conventional principal types and wildcards used by Apache Fluss 1.0.
const (
	ACLPrincipalUser     ACLPrincipalType = "User"
	ACLPrincipalGroup    ACLPrincipalType = "Group"
	ACLPrincipalRole     ACLPrincipalType = "Role"
	ACLPrincipalWildcard ACLPrincipalType = "*"

	ACLWildcardResourceName  = "*"
	ACLWildcardHost          = "*"
	ACLWildcardPrincipalName = "*"
	ACLClusterResourceName   = "fluss-cluster"
)

// ACLBinding describes one Fluss access-control entry.
type ACLBinding struct {
	// ResourceName is a concrete database, table, or cluster resource.
	ResourceName string
	// ResourceType must be a concrete resource type, not [ACLResourceAny].
	ResourceType ACLResourceType
	// PrincipalName is a concrete name or [ACLWildcardPrincipalName].
	PrincipalName string
	// PrincipalType identifies the principal namespace.
	PrincipalType ACLPrincipalType
	// Host is a concrete host or [ACLWildcardHost].
	Host string
	// Operation must be a concrete operation, not [ACLOperationAny].
	Operation ACLOperation
	// Permission must be [ACLPermissionAllow] for creation.
	Permission ACLPermission
}

func (a ACLBinding) validate() error {
	if a.ResourceName == "" {
		return fmt.Errorf("%w: ACLBinding resource name is required", fgo.ErrInvalidConfig)
	}
	if !a.ResourceType.valid(false) {
		return fmt.Errorf("%w: ACLBinding resource type %d is not concrete", fgo.ErrInvalidConfig, a.ResourceType)
	}
	if err := validateACLPrincipal(a.PrincipalName, a.PrincipalType); err != nil {
		return err
	}
	if a.Host == "" {
		return fmt.Errorf("%w: ACLBinding host is required", fgo.ErrInvalidConfig)
	}
	if !a.Operation.valid(false) {
		return fmt.Errorf("%w: ACLBinding operation %d is not concrete", fgo.ErrInvalidConfig, a.Operation)
	}
	if a.Permission != ACLPermissionAllow {
		return fmt.Errorf("%w: ACLBinding permission %d is not concrete", fgo.ErrInvalidConfig, a.Permission)
	}
	return nil
}

func (a ACLBinding) message() *fmsg.PbAclInfo {
	return &fmsg.PbAclInfo{
		ResourceName: proto.String(a.ResourceName), ResourceType: proto.Int32(int32(a.ResourceType)),
		PrincipalName: proto.String(a.PrincipalName), PrincipalType: proto.String(string(a.PrincipalType)),
		Host: proto.String(a.Host), OperationType: proto.Int32(int32(a.Operation)),
		PermissionType: proto.Int32(int32(a.Permission)),
	}
}

// ACLBindingFilter selects access-control entries.
// Nil optional string fields and explicit Any enum values act as wildcards.
// PrincipalName and PrincipalType must either both be nil or both be set.
type ACLBindingFilter struct {
	// ResourceName is nil to match every resource name.
	ResourceName *string
	// ResourceType may be [ACLResourceAny].
	ResourceType ACLResourceType
	// PrincipalName is nil only when PrincipalType is also nil.
	PrincipalName *string
	// PrincipalType is nil only when PrincipalName is also nil.
	PrincipalType *ACLPrincipalType
	// Host is nil to match every host.
	Host *string
	// Operation may be [ACLOperationAny].
	Operation ACLOperation
	// Permission may be [ACLPermissionAny].
	Permission ACLPermission
}

func (f ACLBindingFilter) validate() error {
	if !f.ResourceType.valid(true) {
		return fmt.Errorf("%w: invalid ACLBinding filter resource type %d", fgo.ErrInvalidConfig, f.ResourceType)
	}
	if f.ResourceName != nil && *f.ResourceName == "" {
		return fmt.Errorf("%w: ACLBinding filter resource name is empty", fgo.ErrInvalidConfig)
	}
	if (f.PrincipalName == nil) != (f.PrincipalType == nil) {
		return fmt.Errorf("%w: ACLBinding filter principal name and type must be set together", fgo.ErrInvalidConfig)
	}
	if f.PrincipalName != nil {
		if err := validateACLPrincipal(*f.PrincipalName, *f.PrincipalType); err != nil {
			return err
		}
	}
	if f.Host != nil && *f.Host == "" {
		return fmt.Errorf("%w: ACLBinding filter host is empty", fgo.ErrInvalidConfig)
	}
	if !f.Operation.valid(true) {
		return fmt.Errorf("%w: invalid ACLBinding filter operation %d", fgo.ErrInvalidConfig, f.Operation)
	}
	if !f.Permission.valid(true) {
		return fmt.Errorf("%w: invalid ACLBinding filter permission %d", fgo.ErrInvalidConfig, f.Permission)
	}
	return nil
}

func (f ACLBindingFilter) message() *fmsg.PbAclFilter {
	return &fmsg.PbAclFilter{
		ResourceName: f.ResourceName, ResourceType: proto.Int32(int32(f.ResourceType)),
		PrincipalName: f.PrincipalName, PrincipalType: aclPrincipalTypeString(f.PrincipalType), Host: f.Host,
		OperationType: proto.Int32(int32(f.Operation)), PermissionType: proto.Int32(int32(f.Permission)),
	}
}

func (t ACLResourceType) valid(allowAny bool) bool {
	return (allowAny && t == ACLResourceAny) ||
		t == ACLResourceCluster ||
		t == ACLResourceDatabase ||
		t == ACLResourceTable
}

func (o ACLOperation) valid(allowAny bool) bool {
	return (allowAny && o == ACLOperationAny) ||
		(o >= ACLOperationAll && o <= ACLOperationDescribe)
}

func (p ACLPermission) valid(allowAny bool) bool {
	return (allowAny && p == ACLPermissionAny) || p == ACLPermissionAllow
}

func validateACLPrincipal(name string, principalType ACLPrincipalType) error {
	if name == "" {
		return fmt.Errorf("%w: ACLBinding principal name is required", fgo.ErrInvalidConfig)
	}
	value := string(principalType)
	if value == "" || strings.TrimSpace(value) != value {
		return fmt.Errorf("%w: invalid ACLBinding principal type %q", fgo.ErrInvalidConfig, value)
	}
	for _, canonical := range []ACLPrincipalType{
		ACLPrincipalUser,
		ACLPrincipalGroup,
		ACLPrincipalRole,
		ACLPrincipalWildcard,
	} {
		if strings.EqualFold(value, string(canonical)) && principalType != canonical {
			return fmt.Errorf(
				"%w: ACLBinding principal type %q must use canonical form %q",
				fgo.ErrInvalidConfig,
				value,
				canonical,
			)
		}
	}
	if (name == ACLWildcardPrincipalName) != (principalType == ACLPrincipalWildcard) {
		return fmt.Errorf(
			"%w: wildcard ACLBinding principal name and type must be used together",
			fgo.ErrInvalidConfig,
		)
	}
	return nil
}

func aclPrincipalTypeString(principalType *ACLPrincipalType) *string {
	if principalType == nil {
		return nil
	}
	value := string(*principalType)
	return &value
}

// CreateACLResult associates one ACLBinding with its server-side result.
type CreateACLResult struct {
	// Binding is the server-returned access-control entry.
	Binding ACLBinding
	// Err is the entry-local server failure.
	Err error
}

// CreateACLs creates entries and returns one result per input ACL binding.
func (c *Client) CreateACLs(ctx context.Context, acls ...ACLBinding) ([]CreateACLResult, error) {
	if len(acls) == 0 {
		return nil, fmt.Errorf("%w: no ACLs", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyCreateAcls, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.CreateAclsRequest)
	for _, acl := range acls {
		if err := acl.validate(); err != nil {
			return nil, err
		}
		message.Acl = append(message.Acl, acl.message())
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return nil, err
	}
	created, ok := response.Message().(*fmsg.CreateAclsResponse)
	if !ok {
		return nil, unexpected("create ACLs", response)
	}
	if len(created.GetAclRes()) != len(acls) {
		return nil, fmt.Errorf("%w: create ACLBinding response count mismatch", fgo.ErrValidation)
	}
	results := make([]CreateACLResult, len(acls))
	for index, item := range created.GetAclRes() {
		acl, err := aclFromMessage(item.GetAcl())
		if err != nil {
			return nil, err
		}
		results[index] = CreateACLResult{
			Binding: acl,
			Err:     fgo.ResponseError(item.GetErrorCode(), item.GetErrorMessage(), fmsg.APIKeyCreateAcls),
		}
	}
	return results, nil
}

// ListACLs returns entries matching filter.
func (c *Client) ListACLs(ctx context.Context, filter ACLBindingFilter) ([]ACLBinding, error) {
	if err := filter.validate(); err != nil {
		return nil, err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyListAcls, 0)
	if err != nil {
		return nil, err
	}
	request.Message().(*fmsg.ListAclsRequest).AclFilter = filter.message()
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return nil, err
	}
	list, ok := response.Message().(*fmsg.ListAclsResponse)
	if !ok {
		return nil, unexpected("list ACLs", response)
	}
	acls := make([]ACLBinding, len(list.GetAcl()))
	for index, item := range list.GetAcl() {
		acl, err := aclFromMessage(item)
		if err != nil {
			return nil, err
		}
		acls[index] = acl
	}
	return acls, nil
}

// DropACLResult contains matches and errors for one requested filter.
type DropACLResult struct {
	// Filter is the requested filter associated with this result.
	Filter ACLBindingFilter
	// Matches contains independently reported matching ACLBinding outcomes.
	Matches []CreateACLResult
	// Err is the filter-level server failure.
	Err error
}

// DropACLs removes entries and returns one result per input filter.
func (c *Client) DropACLs(ctx context.Context, filters ...ACLBindingFilter) ([]DropACLResult, error) {
	if len(filters) == 0 {
		return nil, fmt.Errorf("%w: no ACLBinding filters", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyDropAcls, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.DropAclsRequest)
	for _, filter := range filters {
		if err := filter.validate(); err != nil {
			return nil, err
		}
		message.AclFilter = append(message.AclFilter, filter.message())
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return nil, err
	}
	dropped, ok := response.Message().(*fmsg.DropAclsResponse)
	if !ok {
		return nil, unexpected("drop ACLs", response)
	}
	if len(dropped.GetFilterResults()) != len(filters) {
		return nil, fmt.Errorf("%w: drop ACLBinding response count mismatch", fgo.ErrValidation)
	}
	results := make([]DropACLResult, len(filters))
	for index, item := range dropped.GetFilterResults() {
		results[index].Filter = filters[index]
		results[index].Err = fgo.ResponseError(item.GetErrorCode(), item.GetErrorMessage(), fmsg.APIKeyDropAcls)
		for _, match := range item.GetMatchingAcls() {
			acl, err := aclFromMessage(match.GetAcl())
			if err != nil {
				return nil, err
			}
			results[index].Matches = append(results[index].Matches, CreateACLResult{
				Binding: acl,
				Err:     fgo.ResponseError(match.GetErrorCode(), match.GetErrorMessage(), fmsg.APIKeyDropAcls),
			})
		}
	}
	return results, nil
}

func aclFromMessage(message *fmsg.PbAclInfo) (ACLBinding, error) {
	if message == nil {
		return ACLBinding{}, fmt.Errorf("%w: missing ACLBinding in server response", fgo.ErrValidation)
	}
	acl := ACLBinding{
		ResourceName:  message.GetResourceName(),
		ResourceType:  ACLResourceType(message.GetResourceType()),
		PrincipalName: message.GetPrincipalName(),
		PrincipalType: ACLPrincipalType(message.GetPrincipalType()),
		Host:          message.GetHost(),
		Operation:     ACLOperation(message.GetOperationType()),
		Permission:    ACLPermission(message.GetPermissionType()),
	}
	if err := acl.validate(); err != nil {
		return ACLBinding{}, fmt.Errorf("%w: malformed ACLBinding server response: %v", fgo.ErrValidation, err)
	}
	return acl, nil
}
