package release

import (
	"fmt"
	"strings"
)

func RequiredImage(lookup func(string) string) (string, error) {
	image := lookup("RELEASE_IMAGE")
	if strings.TrimSpace(image) == "" {
		return "", fmt.Errorf("release image environment variable RELEASE_IMAGE must not be empty")
	}
	return image, nil
}
