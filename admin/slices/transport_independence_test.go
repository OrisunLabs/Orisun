package slices_test

import (
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/OrisunLabs/Orisun"

func TestAdminSlicesAndEmbeddedPackagesDoNotDependOnGRPCOrProtobuf(t *testing.T) {
	root := repositoryRoot(t)
	testCases := []struct {
		name string
		args []string
	}{
		{
			name: "admin slices",
			args: []string{"./admin/slices/..."},
		},
		{
			name: "embedded packages",
			args: []string{"./embedded/..."},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			dependencies := listDependencies(t, root, testCase.args...)
			var forbidden []string
			for _, dependency := range dependencies {
				if isGRPCOrProtobufPackage(dependency) {
					forbidden = append(forbidden, dependency)
				}
			}
			if len(forbidden) != 0 {
				t.Fatalf(
					"transport dependencies leaked into %s:\n%s",
					testCase.name,
					strings.Join(forbidden, "\n"),
				)
			}
		})
	}
}

func TestBoundarySlicesRemainTransportIndependent(t *testing.T) {
	root := repositoryRoot(t)
	roots := []string{
		modulePath + "/admin/slices/create_boundary",
		modulePath + "/admin/slices/boundary_catalog",
		modulePath + "/admin/slices/boundary_provisioning",
	}
	visited := make(map[string]bool)
	for _, packagePath := range roots {
		checkTransportIndependentPackage(t, root, packagePath, visited)
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate architecture test")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", ".."))
}

func listDependencies(t *testing.T, root string, args ...string) []string {
	t.Helper()
	commandArgs := append([]string{"list", "-deps", "-test"}, args...)
	command := exec.Command("go", commandArgs...)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(commandArgs, " "), err, output)
	}
	return strings.Fields(string(output))
}

func isGRPCOrProtobufPackage(importPath string) bool {
	for _, root := range []string{
		"google.golang.org/grpc",
		"google.golang.org/protobuf",
	} {
		if importPath == root || strings.HasPrefix(importPath, root+"/") {
			return true
		}
	}
	return false
}

func checkTransportIndependentPackage(t *testing.T, root, packagePath string, visited map[string]bool) {
	t.Helper()
	if visited[packagePath] {
		return
	}
	visited[packagePath] = true

	relative := strings.TrimPrefix(packagePath, modulePath+"/")
	directory := filepath.Join(root, filepath.FromSlash(relative))
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read %s: %v", packagePath, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		filename := filepath.Join(directory, entry.Name())
		file, err := parser.ParseFile(token.NewFileSet(), filename, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse imports in %s: %v", filename, err)
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("decode import in %s: %v", filename, err)
			}
			if forbiddenTransportImport(importPath) {
				t.Errorf("%s imports forbidden transport package %s", filename, importPath)
			}
			if strings.HasPrefix(importPath, modulePath+"/") {
				checkTransportIndependentPackage(t, root, importPath, visited)
			}
		}
	}
}

func forbiddenTransportImport(importPath string) bool {
	return importPath == modulePath+"/orisun" ||
		strings.HasPrefix(importPath, modulePath+"/orisun/") ||
		isGRPCOrProtobufPackage(importPath)
}
