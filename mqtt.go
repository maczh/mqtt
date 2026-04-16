package mqtt

import (
	"errors"
	"math/rand"
	"strings"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/sadlil/gologger"
)

type mqtt struct {
	configData []byte
	conf       *koanf.Koanf
	client     paho.Client
	// Topics      []SubTopics
	multi       bool
	tags        []string
	connections map[string]*connection
}

type connection struct {
	Client   paho.Client
	Broker   string
	ClientId string
	Username string
	Password string
	Topics   []SubTopics
}

type SubTopics struct {
	Topic       string
	Qos         byte
	HandlerFunc *paho.MessageHandler
}

var MQTT = &mqtt{}
var logger = gologger.GetLogger()

func (m *mqtt) Init(configData []byte) {
	m.configData = configData
	m.conf = koanf.New(".")
	if err := m.conf.Load(rawbytes.Provider(m.configData), yaml.Parser()); err != nil {
		logger.Error("load config failed, err: " + err.Error())
		return
	}
	if m.connections == nil || len(m.connections) == 0 {
		m.connections = make(map[string]*connection)
		m.tags = make([]string, 0)
		m.multi = m.conf.Bool("go.data.mqtt.multi")
		if m.multi {
			tags := strings.Split(m.conf.String("go.data.mqtt.conns"), ",")
			m.tags = tags
			for _, tag := range tags {
				conn := &connection{
					Broker:   m.conf.String("go.data.mqtt." + tag + ".broker"),
					ClientId: m.conf.String("go.data.mqtt."+tag+".clientId") + "-" + generateRandHexString(4),
					Username: m.conf.String("go.data.mqtt." + tag + ".username"),
					Password: m.conf.String("go.data.mqtt." + tag + ".password"),
					Topics:   make([]SubTopics, 0),
				}
				conn.Client = paho.NewClient(paho.NewClientOptions().SetCleanSession(false).SetAutoReconnect(true).AddBroker(conn.Broker).SetClientID(conn.ClientId).SetUsername(conn.Username).SetPassword(conn.Password).SetOnConnectHandler(conn.onConnectHandler))
				if token := conn.Client.Connect(); token.Wait() && token.Error() != nil {
					logger.Error("connect mqtt broker failed, tag: " + tag + ", err: " + token.Error().Error())
					continue
				}
				m.connections[tag] = conn
				logger.Info("connect mqtt broker success, tag: " + tag + ", broker: " + conn.Broker + ", clientId: " + conn.ClientId)
			}
		} else {
			conn := &connection{
				Broker:   m.conf.String("go.data.mqtt.broker"),
				ClientId: m.conf.String("go.data.mqtt.clientId") + "-" + generateRandHexString(4),
				Username: m.conf.String("go.data.mqtt.username"),
				Password: m.conf.String("go.data.mqtt.password"),
				Topics:   make([]SubTopics, 0),
			}
			conn.Client = paho.NewClient(paho.NewClientOptions().SetCleanSession(false).SetAutoReconnect(true).AddBroker(conn.Broker).SetClientID(conn.ClientId).SetUsername(conn.Username).SetPassword(conn.Password).SetOnConnectHandler(conn.onConnectHandler))
			if token := conn.Client.Connect(); token.Wait() && token.Error() != nil {
				logger.Error("connect mqtt broker failed, err: " + token.Error().Error())
				return
			}
			m.connections["0"] = conn
			logger.Info("connect mqtt broker success, broker: " + conn.Broker + ", clientId: " + conn.ClientId)
		}
	}
}

func (c *connection) onConnectHandler(client paho.Client) {
	if len(c.Topics) > 0 {
		logger.Info("重新连接成功，重新订阅主题...")
		for _, topic := range c.Topics {
			token := c.Client.Subscribe(topic.Topic, topic.Qos, safeHandler(*topic.HandlerFunc))
			if token.Error() != nil {
				logger.Error("subscribe topic failed, topic: " + topic.Topic + ", err: " + token.Error().Error())
				return
			}
			token.Wait()
		}
	}
}

func (m *mqtt) GetConnection(tag ...string) (*connection, error) {
	if !m.multi {
		if m.connections["0"].Client.IsConnected() {
			return m.connections["0"], nil
		} else {
			conn := m.connections["0"]
			conn.Client = paho.NewClient(paho.NewClientOptions().SetCleanSession(false).SetAutoReconnect(true).AddBroker(conn.Broker).SetClientID(conn.ClientId).SetUsername(conn.Username).SetPassword(conn.Password).SetOnConnectHandler(conn.onConnectHandler))
			if token := conn.Client.Connect(); token.Wait() && token.Error() != nil {
				logger.Error("reconnect mqtt broker failed, err: " + token.Error().Error())
				return nil, token.Error()
			}
			m.connections["0"] = conn
			logger.Info("reconnect mqtt broker success, broker: " + conn.Broker + ", clientId: " + conn.ClientId)
			return conn, nil
		}
	}
	if len(tag) == 0 || tag[0] == "" {
		return nil, errors.New("tag is required for multi connections")
	}
	if _, ok := m.connections[tag[0]]; !ok {
		return nil, errors.New("connection not found for tag: " + tag[0])
	}
	if m.connections[tag[0]].Client.IsConnected() {
		return m.connections[tag[0]], nil
	} else {
		conn := m.connections[tag[0]]
		conn.Client = paho.NewClient(paho.NewClientOptions().SetCleanSession(false).SetAutoReconnect(true).AddBroker(conn.Broker).SetClientID(conn.ClientId).SetUsername(conn.Username).SetPassword(conn.Password).SetOnConnectHandler(conn.onConnectHandler))
		if token := conn.Client.Connect(); token.Wait() && token.Error() != nil {
			logger.Error("reconnect mqtt broker failed, tag: " + tag[0] + ", err: " + token.Error().Error())
			return nil, token.Error()
		}
		m.connections[tag[0]] = conn
		return conn, nil
	}

}

func (m *mqtt) Close() {
	if !m.multi {
		if len(m.connections["0"].Topics) > 0 {
			topics := make([]string, 0)
			for _, topic := range m.connections["0"].Topics {
				topics = append(topics, topic.Topic)
			}
			m.connections["0"].Client.Unsubscribe(topics...)
		}
		m.connections["0"].Client.Disconnect(0)
		logger.Info("disconnect mqtt broker success, broker: " + m.connections["0"].Broker + ", clientId: " + m.connections["0"].ClientId)
		delete(m.connections, "0")
	} else {
		for tag, _ := range m.connections {
			if len(m.connections[tag].Topics) > 0 {
				topics := make([]string, 0)
				for _, topic := range m.connections[tag].Topics {
					topics = append(topics, topic.Topic)
				}
				m.connections[tag].Client.Unsubscribe(topics...)
			}
			m.connections[tag].Client.Disconnect(0)
			logger.Info("disconnect mqtt broker success, tag: " + tag + ", broker: " + m.connections[tag].Broker + ", clientId: " + m.connections[tag].ClientId)
			delete(m.connections, tag)
		}
	}
}

func (m *mqtt) Check() error {
	var err error
	if m.multi {
		for tag, conn := range m.connections {
			if !conn.Client.IsConnected() {
				logger.Error("mqtt client not connected, tag: " + tag)
				conn.Client = paho.NewClient(paho.NewClientOptions().SetCleanSession(false).SetAutoReconnect(true).AddBroker(conn.Broker).SetClientID(conn.ClientId).SetUsername(conn.Username).SetPassword(conn.Password).SetOnConnectHandler(conn.onConnectHandler))
				if token := conn.Client.Connect(); token.Wait() && token.Error() != nil {
					logger.Error("reconnect mqtt broker failed, tag: " + tag + ", err: " + token.Error().Error())
					err = token.Error()
				}
				m.connections[tag] = conn
			}
		}
	} else {
		conn := m.connections["0"]
		if !conn.Client.IsConnected() {
			logger.Error("mqtt client not connected")
			conn.Client = paho.NewClient(paho.NewClientOptions().SetCleanSession(false).SetAutoReconnect(true).AddBroker(conn.Broker).SetClientID(conn.ClientId).SetUsername(conn.Username).SetPassword(conn.Password).SetOnConnectHandler(conn.onConnectHandler))
			if token := conn.Client.Connect(); token.Wait() && token.Error() != nil {
				logger.Error("reconnect mqtt broker failed, err: " + token.Error().Error())
				err = token.Error()
			}
			m.connections["0"] = conn
		}
	}
	return err
}

// safeHandler 包装MessageHandler，捕获panic
func safeHandler(handler paho.MessageHandler) paho.MessageHandler {
	return func(client paho.Client, msg paho.Message) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("mqtt message handler panic: " + msg.Topic() + ", error: " + r.(string))
			}
		}()
		handler(client, msg)
	}
}

func (m *mqtt) Subscribe(tag, topic string, qos byte, handlerFunc paho.MessageHandler) error {
	conn, err := m.GetConnection(tag)
	if err != nil {
		return err
	}
	// 使用安全包装的handler
	token := conn.Client.Subscribe(topic, qos, safeHandler(handlerFunc))
	if token.Error() != nil {
		logger.Error("subscribe topic failed, topic: " + topic + ", err: " + token.Error().Error())
		return token.Error()
	}
	token.Wait()
	if tag == "" {
		tag = "0"
	}
	m.connections[tag].Topics = append(m.connections[tag].Topics, SubTopics{Topic: topic, Qos: qos, HandlerFunc: &handlerFunc})
	return nil
}

func (m *mqtt) SubscribeMultiple(tag string, filters map[string]byte, callback paho.MessageHandler) error {
	conn, err := m.GetConnection(tag)
	if err != nil {
		return err
	}
	// 使用安全包装的handler
	token := conn.Client.SubscribeMultiple(filters, safeHandler(callback))
	if token.Error() != nil {
		logger.Error("subscribe Topics failed, err: " + token.Error().Error())
		return token.Error()
	}
	token.Wait()
	if tag == "" {
		tag = "0"
	}
	for topic, _ := range filters {
		m.connections[tag].Topics = append(m.connections[tag].Topics, SubTopics{Topic: topic, Qos: filters[topic], HandlerFunc: &callback})
	}
	return nil
}

func (m *mqtt) Publish(tag, topic string, qos byte, retained bool, payload interface{}) error {
	conn, err := m.GetConnection(tag)
	if err != nil {
		return err
	}
	if token := conn.Client.Publish(topic, qos, retained, payload); token.Wait() && token.Error() != nil {
		logger.Error("publish topic failed, topic: " + topic + ", err: " + token.Error().Error())
		return token.Error()
	}
	return nil
}
func (m *mqtt) UnSubscribe(tag string, topics ...string) error {
	conn, err := m.GetConnection(tag)
	if err != nil {
		return err
	}
	if token := conn.Client.Unsubscribe(topics...); token.Wait() && token.Error() != nil {
		logger.Error("unsubscribe Topics failed, err: " + token.Error().Error())
		return token.Error()
	}
	return nil
}

func generateRandHexString(sl int) string {
	source := []byte("0123456789abcdef")
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < sl; i++ {
		result = append(result, source[r.Intn(len(source))])
	}
	return string(result)
}
