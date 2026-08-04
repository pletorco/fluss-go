package fadm

import (
	"errors"
	"strings"
	"testing"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func validACL() ACLBinding {
	return ACLBinding{
		ResourceName:  "analytics",
		ResourceType:  ACLResourceDatabase,
		PrincipalName: "alice",
		PrincipalType: ACLPrincipalUser,
		Host:          ACLWildcardHost,
		Operation:     ACLOperationDescribe,
		Permission:    ACLPermissionAllow,
	}
}

func validACLBindingFilter() ACLBindingFilter {
	return ACLBindingFilter{
		ResourceType: ACLResourceAny,
		Operation:    ACLOperationAny,
		Permission:   ACLPermissionAny,
	}
}

func TestACLResourceTypeWireValues(t *testing.T) {
	resourceTypes := map[ACLResourceType]int32{
		ACLResourceAny:      1,
		ACLResourceCluster:  2,
		ACLResourceDatabase: 3,
		ACLResourceTable:    4,
	}
	for resourceType, want := range resourceTypes {
		if got := int32(resourceType); got != want {
			t.Errorf("resource type %v = %d, want %d", resourceType, got, want)
		}
		if resourceType != ACLResourceAny {
			acl := validACL()
			acl.ResourceType = resourceType
			if err := acl.validate(); err != nil {
				t.Errorf("resource type %v validation: %v", resourceType, err)
			}
			if got := acl.message().GetResourceType(); got != want {
				t.Errorf("resource type %v message = %d, want %d", resourceType, got, want)
			}
		}
	}
}

func TestACLOperationWireValues(t *testing.T) {
	operations := map[ACLOperation]int32{
		ACLOperationAny:      1,
		ACLOperationAll:      2,
		ACLOperationRead:     3,
		ACLOperationWrite:    4,
		ACLOperationCreate:   5,
		ACLOperationDrop:     6,
		ACLOperationAlter:    7,
		ACLOperationDescribe: 8,
	}
	for operation, want := range operations {
		if got := int32(operation); got != want {
			t.Errorf("operation %v = %d, want %d", operation, got, want)
		}
		if operation != ACLOperationAny {
			acl := validACL()
			acl.Operation = operation
			if err := acl.validate(); err != nil {
				t.Errorf("operation %v validation: %v", operation, err)
			}
			if got := acl.message().GetOperationType(); got != want {
				t.Errorf("operation %v message = %d, want %d", operation, got, want)
			}
		}
	}
}

func TestACLPermissionWireValues(t *testing.T) {
	permissions := map[ACLPermission]int32{
		ACLPermissionAny:   1,
		ACLPermissionAllow: 2,
	}
	for permission, want := range permissions {
		if got := int32(permission); got != want {
			t.Errorf("permission %v = %d, want %d", permission, got, want)
		}
	}
}

func TestACLPrincipalTypes(t *testing.T) {
	for _, principalType := range []ACLPrincipalType{
		ACLPrincipalUser,
		ACLPrincipalGroup,
		ACLPrincipalRole,
		"ServiceAccount",
	} {
		acl := validACL()
		acl.PrincipalType = principalType
		if err := acl.validate(); err != nil {
			t.Errorf("principal type %q validation: %v", principalType, err)
		}
		if got := acl.message().GetPrincipalType(); got != string(principalType) {
			t.Errorf("principal type message = %q, want %q", got, principalType)
		}
	}
}

func TestACLValidationAndMessage(t *testing.T) {
	acl := validACL()
	if err := acl.validate(); err != nil {
		t.Fatal(err)
	}
	message := acl.message()
	if message.GetResourceType() != 3 ||
		message.GetOperationType() != 8 ||
		message.GetPermissionType() != 2 ||
		message.GetPrincipalType() != "User" {
		t.Fatalf("ACLBinding message = %#v", message)
	}

	tests := map[string]func(*ACLBinding){
		"resource name":    func(acl *ACLBinding) { acl.ResourceName = "" },
		"resource zero":    func(acl *ACLBinding) { acl.ResourceType = 0 },
		"resource unknown": func(acl *ACLBinding) { acl.ResourceType = 99 },
		"resource any":     func(acl *ACLBinding) { acl.ResourceType = ACLResourceAny },
		"principal name":   func(acl *ACLBinding) { acl.PrincipalName = "" },
		"principal type":   func(acl *ACLBinding) { acl.PrincipalType = "" },
		"principal spaces": func(acl *ACLBinding) { acl.PrincipalType = " User " },
		"principal case":   func(acl *ACLBinding) { acl.PrincipalType = "USER" },
		"wildcard name": func(acl *ACLBinding) {
			acl.PrincipalName = ACLWildcardPrincipalName
		},
		"wildcard type":     func(acl *ACLBinding) { acl.PrincipalType = ACLPrincipalWildcard },
		"host":              func(acl *ACLBinding) { acl.Host = "" },
		"operation zero":    func(acl *ACLBinding) { acl.Operation = 0 },
		"operation unknown": func(acl *ACLBinding) { acl.Operation = 99 },
		"operation any":     func(acl *ACLBinding) { acl.Operation = ACLOperationAny },
		"permission zero":   func(acl *ACLBinding) { acl.Permission = 0 },
		"permission unknown": func(acl *ACLBinding) {
			acl.Permission = 99
		},
		"permission any": func(acl *ACLBinding) { acl.Permission = ACLPermissionAny },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			invalid := validACL()
			mutate(&invalid)
			if err := invalid.validate(); !errors.Is(err, fgo.ErrInvalidConfig) {
				t.Fatalf("validate() error = %v", err)
			}
		})
	}

	custom := validACL()
	custom.PrincipalType = "ServiceAccount"
	if err := custom.validate(); err != nil {
		t.Fatalf("custom principal type: %v", err)
	}

	wildcard := validACL()
	wildcard.PrincipalName = ACLWildcardPrincipalName
	wildcard.PrincipalType = ACLPrincipalWildcard
	if err := wildcard.validate(); err != nil {
		t.Fatalf("wildcard principal: %v", err)
	}
}

func TestACLBindingFilterValidationAndMessage(t *testing.T) {
	filter := validACLBindingFilter()
	if err := filter.validate(); err != nil {
		t.Fatal(err)
	}
	message := filter.message()
	if message.GetResourceType() != 1 ||
		message.GetOperationType() != 1 ||
		message.GetPermissionType() != 1 ||
		message.PrincipalName != nil ||
		message.PrincipalType != nil {
		t.Fatalf("ACLBinding filter message = %#v", message)
	}

	resourceName := "analytics"
	principalName := "alice"
	principalType := ACLPrincipalUser
	host := ACLWildcardHost
	filter = ACLBindingFilter{
		ResourceName:  &resourceName,
		ResourceType:  ACLResourceDatabase,
		PrincipalName: &principalName,
		PrincipalType: &principalType,
		Host:          &host,
		Operation:     ACLOperationDescribe,
		Permission:    ACLPermissionAllow,
	}
	if err := filter.validate(); err != nil {
		t.Fatal(err)
	}
	message = filter.message()
	if message.GetPrincipalType() != "User" || message.GetPrincipalName() != "alice" {
		t.Fatalf("specific ACLBinding filter message = %#v", message)
	}

	empty := ""
	upperUser := ACLPrincipalType("USER")
	tests := map[string]ACLBindingFilter{
		"zero":                {},
		"resource unknown":    {ResourceType: 99, Operation: ACLOperationAny, Permission: ACLPermissionAny},
		"resource name empty": {ResourceName: &empty, ResourceType: ACLResourceAny, Operation: ACLOperationAny, Permission: ACLPermissionAny},
		"principal name only": {PrincipalName: &principalName, ResourceType: ACLResourceAny, Operation: ACLOperationAny, Permission: ACLPermissionAny},
		"principal type only": {PrincipalType: &principalType, ResourceType: ACLResourceAny, Operation: ACLOperationAny, Permission: ACLPermissionAny},
		"principal case":      {PrincipalName: &principalName, PrincipalType: &upperUser, ResourceType: ACLResourceAny, Operation: ACLOperationAny, Permission: ACLPermissionAny},
		"host empty":          {Host: &empty, ResourceType: ACLResourceAny, Operation: ACLOperationAny, Permission: ACLPermissionAny},
		"operation unknown":   {ResourceType: ACLResourceAny, Operation: 99, Permission: ACLPermissionAny},
		"permission unknown":  {ResourceType: ACLResourceAny, Operation: ACLOperationAny, Permission: 99},
	}
	for name, invalid := range tests {
		t.Run(name, func(t *testing.T) {
			if err := invalid.validate(); !errors.Is(err, fgo.ErrInvalidConfig) {
				t.Fatalf("validate() error = %v", err)
			}
		})
	}
}

func TestACLFromMessageValidation(t *testing.T) {
	acl := validACL()
	decoded, err := aclFromMessage(acl.message())
	if err != nil || decoded != acl {
		t.Fatalf("aclFromMessage() = %#v, %v", decoded, err)
	}

	tests := map[string]*fmsg.PbAclInfo{
		"nil": nil,
		"unknown resource": {
			ResourceName: proto.String("analytics"), ResourceType: proto.Int32(99),
			PrincipalName: proto.String("alice"), PrincipalType: proto.String("User"),
			Host: proto.String("*"), OperationType: proto.Int32(8), PermissionType: proto.Int32(2),
		},
		"unknown operation": {
			ResourceName: proto.String("analytics"), ResourceType: proto.Int32(3),
			PrincipalName: proto.String("alice"), PrincipalType: proto.String("User"),
			Host: proto.String("*"), OperationType: proto.Int32(99), PermissionType: proto.Int32(2),
		},
		"unknown permission": {
			ResourceName: proto.String("analytics"), ResourceType: proto.Int32(3),
			PrincipalName: proto.String("alice"), PrincipalType: proto.String("User"),
			Host: proto.String("*"), OperationType: proto.Int32(8), PermissionType: proto.Int32(99),
		},
		"noncanonical principal": {
			ResourceName: proto.String("analytics"), ResourceType: proto.Int32(3),
			PrincipalName: proto.String("alice"), PrincipalType: proto.String("USER"),
			Host: proto.String("*"), OperationType: proto.Int32(8), PermissionType: proto.Int32(2),
		},
	}
	for name, message := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := aclFromMessage(message)
			if !errors.Is(err, fgo.ErrValidation) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("aclFromMessage() error = %v", err)
			}
		})
	}
}
