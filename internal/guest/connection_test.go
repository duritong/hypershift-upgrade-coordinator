package guest

import (
	"testing"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestUnsafeKubeconfigsRejectedBeforeTransport(t *testing.T) {
	_, ca := servingCertificate(t)
	base := kubeconfig(t, "https://guest.example", serviceName, ca)
	cases := map[string]func(*clientcmdapi.Config){
		"exec": func(k *clientcmdapi.Config) {
			k.AuthInfos["observer"].Exec = &clientcmdapi.ExecConfig{Command: "must-not-execute"}
		},
		"auth-provider": func(k *clientcmdapi.Config) {
			k.AuthInfos["observer"].AuthProvider = &clientcmdapi.AuthProviderConfig{Name: "oidc"}
		},
		"token-file": func(k *clientcmdapi.Config) { k.AuthInfos["observer"].TokenFile = "/must-not-read" },
		"ca-file":    func(k *clientcmdapi.Config) { k.Clusters["guest"].CertificateAuthority = "/must-not-read" },
		"cert-file":  func(k *clientcmdapi.Config) { k.AuthInfos["observer"].ClientCertificate = "/must-not-read" },
		"key-file":   func(k *clientcmdapi.Config) { k.AuthInfos["observer"].ClientKey = "/must-not-read" },
		"admin-cert": func(k *clientcmdapi.Config) {
			k.AuthInfos["observer"].ClientCertificateData = []byte("cert")
			k.AuthInfos["observer"].ClientKeyData = []byte("key")
		},
		"insecure":          func(k *clientcmdapi.Config) { k.Clusters["guest"].InsecureSkipTLSVerify = true },
		"missing-ca":        func(k *clientcmdapi.Config) { k.Clusters["guest"].CertificateAuthorityData = nil },
		"proxy":             func(k *clientcmdapi.Config) { k.Clusters["guest"].ProxyURL = "https://attacker.example" },
		"impersonate":       func(k *clientcmdapi.Config) { k.AuthInfos["observer"].Impersonate = "admin" },
		"impersonate-uid":   func(k *clientcmdapi.Config) { k.AuthInfos["observer"].ImpersonateUID = "admin-uid" },
		"impersonate-group": func(k *clientcmdapi.Config) { k.AuthInfos["observer"].ImpersonateGroups = []string{"system:masters"} },
		"impersonate-extra": func(k *clientcmdapi.Config) {
			k.AuthInfos["observer"].ImpersonateUserExtra = map[string][]string{"key": {"value"}}
		},
		"basic-auth": func(k *clientcmdapi.Config) {
			k.AuthInfos["observer"].Username = "user"
			k.AuthInfos["observer"].Password = "password"
		},
		"no-token": func(k *clientcmdapi.Config) { k.AuthInfos["observer"].Token = "" },
		"http":     func(k *clientcmdapi.Config) { k.Clusters["guest"].Server = "http://guest.example" },
		"userinfo": func(k *clientcmdapi.Config) { k.Clusters["guest"].Server = "https://user:password@guest.example" },
		"query":    func(k *clientcmdapi.Config) { k.Clusters["guest"].Server = "https://guest.example?token=secret" },
		"path":     func(k *clientcmdapi.Config) { k.Clusters["guest"].Server = "https://guest.example/proxy" },
		"unused-plugin": func(k *clientcmdapi.Config) {
			k.AuthInfos["unused"] = &clientcmdapi.AuthInfo{Exec: &clientcmdapi.ExecConfig{Command: "must-not-execute"}}
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			k, err := clientcmd.Load(base)
			if err != nil {
				t.Fatal(err)
			}
			change(k)
			data, err := clientcmd.Write(*k)
			if err != nil {
				t.Fatal(err)
			}
			_, err = ScopedConfig(data)
			requireFailure(t, err, UnsupportedEvidence, "UnsafeKubeconfig")
		})
	}
	if _, err := ScopedConfig(base); err != nil {
		t.Fatalf("safe config rejected: %v", err)
	}
	for _, data := range [][]byte{nil, []byte("not kubeconfig")} {
		_, err := ScopedConfig(data)
		requireFailure(t, err, UnsupportedEvidence, "UnsafeKubeconfig")
	}
}
