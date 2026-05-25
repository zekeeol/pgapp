package pgapp

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	pb "github.com/zekee/pgapp/sdk/go/gen/pgapp/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var ErrNotConnected = errors.New("pgapp: client is not connected")

type Client struct {
	endpoint string
	timeout  time.Duration
	conn     *grpc.ClientConn
	cache    pb.CacheServiceClient
	mq       pb.MQServiceClient
}

func NewClient(endpoint string, timeout time.Duration) *Client {
	return &Client{endpoint: endpoint, timeout: timeout}
}

func Dial(ctx context.Context, endpoint string, timeout time.Duration) (*Client, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := &Client{
		endpoint: endpoint,
		timeout:  timeout,
		conn:     conn,
		cache:    pb.NewCacheServiceClient(conn),
		mq:       pb.NewMQServiceClient(conn),
	}
	if timeout > 0 {
		pingCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		_ = pingCtx
	}
	return client, nil
}

func (c *Client) Endpoint() string {
	return c.endpoint
}

func (c *Client) Timeout() time.Duration {
	return c.timeout
}

type CacheClient struct {
	client *Client
}

func (c *Client) Cache() *CacheClient {
	return &CacheClient{client: c}
}

func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.timeout)
}

type MQClient struct {
	client *Client
}

func (c *Client) MQ() *MQClient {
	return &MQClient{client: c}
}

func (c *CacheClient) Set(ctx context.Context, namespace string, key string, value []byte, ttlSeconds int64) (bool, error) {
	if c.client.cache == nil {
		return false, ErrNotConnected
	}
	ctx, cancel := c.client.withTimeout(ctx)
	defer cancel()
	resp, err := c.client.cache.Set(ctx, &pb.SetCacheRequest{
		Namespace:  namespace,
		Key:        key,
		Value:      value,
		TtlSeconds: ttlSeconds,
	})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

func (c *CacheClient) Get(ctx context.Context, namespace string, key string) ([]byte, bool, error) {
	if c.client.cache == nil {
		return nil, false, ErrNotConnected
	}
	ctx, cancel := c.client.withTimeout(ctx)
	defer cancel()
	resp, err := c.client.cache.Get(ctx, &pb.GetCacheRequest{
		Namespace: namespace,
		Key:       key,
	})
	if err != nil {
		return nil, false, err
	}
	if !resp.Hit {
		return nil, false, nil
	}
	return resp.Value, true, nil
}

func (c *CacheClient) MGet(ctx context.Context, namespace string, keys []string) ([]*pb.CacheItem, error) {
	if c.client.cache == nil {
		return nil, ErrNotConnected
	}
	ctx, cancel := c.client.withTimeout(ctx)
	defer cancel()
	resp, err := c.client.cache.MGet(ctx, &pb.MGetCacheRequest{
		Namespace: namespace,
		Keys:      keys,
	})
	if err != nil {
		return nil, err
	}
	return resp.Items, nil
}

func (c *CacheClient) Delete(ctx context.Context, namespace string, key string) (bool, error) {
	if c.client.cache == nil {
		return false, ErrNotConnected
	}
	ctx, cancel := c.client.withTimeout(ctx)
	defer cancel()
	resp, err := c.client.cache.Delete(ctx, &pb.DeleteCacheRequest{
		Namespace: namespace,
		Key:       key,
	})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

func (c *CacheClient) Exists(ctx context.Context, namespace string, key string) (bool, error) {
	if c.client.cache == nil {
		return false, ErrNotConnected
	}
	ctx, cancel := c.client.withTimeout(ctx)
	defer cancel()
	resp, err := c.client.cache.Exists(ctx, &pb.ExistsCacheRequest{
		Namespace: namespace,
		Key:       key,
	})
	if err != nil {
		return false, err
	}
	return resp.Exists, nil
}

func (c *CacheClient) InvalidateNamespace(ctx context.Context, namespace string) (bool, error) {
	if c.client.cache == nil {
		return false, ErrNotConnected
	}
	ctx, cancel := c.client.withTimeout(ctx)
	defer cancel()
	resp, err := c.client.cache.InvalidateNamespace(ctx, &pb.InvalidateNamespaceRequest{
		Namespace: namespace,
	})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

func (c *CacheClient) Stats(ctx context.Context) (*pb.CacheStatsResponse, error) {
	if c.client.cache == nil {
		return nil, ErrNotConnected
	}
	ctx, cancel := c.client.withTimeout(ctx)
	defer cancel()
	return c.client.cache.Stats(ctx, &pb.CacheStatsRequest{})
}

func (m *MQClient) CreateQueue(ctx context.Context, queueName string) (bool, error) {
	return m.CreateQueueWithMode(ctx, queueName, pb.QueueStorageMode_QUEUE_STORAGE_MODE_DURABLE)
}

func (m *MQClient) CreateQueueWithMode(ctx context.Context, queueName string, storageMode pb.QueueStorageMode) (bool, error) {
	if m.client.mq == nil {
		return false, ErrNotConnected
	}
	ctx, cancel := m.client.withTimeout(ctx)
	defer cancel()
	resp, err := m.client.mq.CreateQueue(ctx, &pb.CreateQueueRequest{
		QueueName:   queueName,
		StorageMode: storageMode,
	})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

func (m *MQClient) SendJSON(ctx context.Context, queueName string, payload interface{}) (int64, error) {
	return m.SendJSONWithDelay(ctx, queueName, payload, 0)
}

func (m *MQClient) SendJSONWithDelay(ctx context.Context, queueName string, payload interface{}, delaySeconds int64) (int64, error) {
	if m.client.mq == nil {
		return 0, ErrNotConnected
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	ctx, cancel := m.client.withTimeout(ctx)
	defer cancel()
	resp, err := m.client.mq.Send(ctx, &pb.SendMessageRequest{
		QueueName:    queueName,
		JsonPayload:  string(data),
		DelaySeconds: delaySeconds,
	})
	if err != nil {
		return 0, err
	}
	return resp.MessageId, nil
}

func (m *MQClient) SendBatchJSON(ctx context.Context, queueName string, payloads []interface{}, delaySeconds int64) ([]int64, error) {
	if m.client.mq == nil {
		return nil, ErrNotConnected
	}
	jsonPayloads := make([]string, 0, len(payloads))
	for _, payload := range payloads {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		jsonPayloads = append(jsonPayloads, string(data))
	}
	ctx, cancel := m.client.withTimeout(ctx)
	defer cancel()
	resp, err := m.client.mq.SendBatch(ctx, &pb.SendBatchRequest{
		QueueName:    queueName,
		JsonPayloads: jsonPayloads,
		DelaySeconds: delaySeconds,
	})
	if err != nil {
		return nil, err
	}
	return resp.MessageIds, nil
}

func (m *MQClient) Read(ctx context.Context, queueName string, quantity int32, visibilityTimeoutSeconds int64) ([]*pb.QueueMessage, error) {
	if m.client.mq == nil {
		return nil, ErrNotConnected
	}
	ctx, cancel := m.client.withTimeout(ctx)
	defer cancel()
	resp, err := m.client.mq.Read(ctx, &pb.ReadMessagesRequest{
		QueueName:                queueName,
		Quantity:                 quantity,
		VisibilityTimeoutSeconds: visibilityTimeoutSeconds,
	})
	if err != nil {
		return nil, err
	}
	return resp.Messages, nil
}

func (m *MQClient) ReadWithPoll(ctx context.Context, queueName string, quantity int32, visibilityTimeoutSeconds int64, maxPollSeconds int64, pollIntervalMillis int64) ([]*pb.QueueMessage, error) {
	if m.client.mq == nil {
		return nil, ErrNotConnected
	}
	ctx, cancel := m.client.withTimeout(ctx)
	defer cancel()
	resp, err := m.client.mq.ReadWithPoll(ctx, &pb.ReadWithPollRequest{
		QueueName:                queueName,
		Quantity:                 quantity,
		VisibilityTimeoutSeconds: visibilityTimeoutSeconds,
		MaxPollSeconds:           maxPollSeconds,
		PollIntervalMillis:       pollIntervalMillis,
	})
	if err != nil {
		return nil, err
	}
	return resp.Messages, nil
}

func (m *MQClient) Delete(ctx context.Context, queueName string, messageID int64) (bool, error) {
	if m.client.mq == nil {
		return false, ErrNotConnected
	}
	ctx, cancel := m.client.withTimeout(ctx)
	defer cancel()
	resp, err := m.client.mq.Delete(ctx, &pb.DeleteMessageRequest{
		QueueName: queueName,
		MessageId: messageID,
	})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

func (m *MQClient) Archive(ctx context.Context, queueName string, messageID int64) (bool, error) {
	if m.client.mq == nil {
		return false, ErrNotConnected
	}
	ctx, cancel := m.client.withTimeout(ctx)
	defer cancel()
	resp, err := m.client.mq.Archive(ctx, &pb.ArchiveMessageRequest{
		QueueName: queueName,
		MessageId: messageID,
	})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

func (m *MQClient) SetVisibilityTimeout(ctx context.Context, queueName string, messageID int64, visibilityTimeoutSeconds int64) (bool, error) {
	if m.client.mq == nil {
		return false, ErrNotConnected
	}
	ctx, cancel := m.client.withTimeout(ctx)
	defer cancel()
	resp, err := m.client.mq.SetVisibilityTimeout(ctx, &pb.SetVisibilityTimeoutRequest{
		QueueName:                queueName,
		MessageId:                messageID,
		VisibilityTimeoutSeconds: visibilityTimeoutSeconds,
	})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

func (m *MQClient) Metrics(ctx context.Context, queueName string) (*pb.QueueMetricsResponse, error) {
	if m.client.mq == nil {
		return nil, ErrNotConnected
	}
	ctx, cancel := m.client.withTimeout(ctx)
	defer cancel()
	return m.client.mq.Metrics(ctx, &pb.QueueMetricsRequest{QueueName: queueName})
}

func (m *MQClient) PurgeQueue(ctx context.Context, queueName string) (bool, error) {
	if m.client.mq == nil {
		return false, ErrNotConnected
	}
	ctx, cancel := m.client.withTimeout(ctx)
	defer cancel()
	resp, err := m.client.mq.PurgeQueue(ctx, &pb.PurgeQueueRequest{QueueName: queueName})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

func (m *MQClient) DropQueue(ctx context.Context, queueName string) (bool, error) {
	if m.client.mq == nil {
		return false, ErrNotConnected
	}
	ctx, cancel := m.client.withTimeout(ctx)
	defer cancel()
	resp, err := m.client.mq.DropQueue(ctx, &pb.DropQueueRequest{QueueName: queueName})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}
