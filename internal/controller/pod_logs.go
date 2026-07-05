package controller

import (
	"context"
	"io"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

type PodLogReader interface {
	Logs(ctx context.Context, namespace, podName string) (string, error)
}

type KubernetesPodLogReader struct {
	Client kubernetes.Interface
}

const (
	failedPodLogTailLines  int64 = 200
	failedPodLogLimitBytes int64 = 128 * 1024
)

func (r KubernetesPodLogReader) Logs(ctx context.Context, namespace, podName string) (string, error) {
	stream, err := r.Client.CoreV1().Pods(namespace).GetLogs(podName, failedPodLogOptions()).Stream(ctx)
	if err != nil {
		return "", err
	}
	data, err := io.ReadAll(stream)
	closeErr := stream.Close()
	if err != nil {
		return "", err
	}
	if closeErr != nil {
		return "", closeErr
	}
	return string(data), nil
}

func failedPodLogOptions() *corev1.PodLogOptions {
	tailLines := failedPodLogTailLines
	limitBytes := failedPodLogLimitBytes
	return &corev1.PodLogOptions{
		TailLines:  &tailLines,
		LimitBytes: &limitBytes,
	}
}
