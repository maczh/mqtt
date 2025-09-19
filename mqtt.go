package mqtt

import (
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
	Topics     []SubTopics
	broker     string
	clientId   string
	username   string
	password   string
}

type SubTopics struct {
	Topic           string
	Qos             byte
	HandlerFuncName string
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
	m.broker = m.conf.String("go.data.mqtt.broker")
	m.clientId = m.conf.String("go.data.mqtt.clientId")
	m.username = m.conf.String("go.data.mqtt.username")
	m.password = m.conf.String("go.data.mqtt.password")
	topics := m.conf.Slices("go.data.mqtt.subscribe")
	for _, topic := range topics {
		m.Topics = append(m.Topics, SubTopics{
			Topic:           topic.String("topic"),
			Qos:             byte(topic.Int("qos")),
			HandlerFuncName: topic.String("handlerFuncName"),
		})
	}
	client := paho.NewClient(paho.NewClientOptions().AddBroker(m.broker).SetClientID(m.clientId).SetUsername(m.username).SetPassword(m.password))
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		logger.Error("connect mqtt broker failed, err: " + token.Error().Error())
		return
	}

	m.client = client

	logger.Info("connect mqtt broker success, broker: " + m.broker + ", clientId: " + m.clientId)
}

func (m *mqtt) Close() {
	if m.client.IsConnected() {
		m.client.Disconnect(0)
		logger.Info("disconnect mqtt broker success, broker: " + m.broker + ", clientId: " + m.clientId)
	}
}

func (m *mqtt) Check() error {
	if !m.client.IsConnected() {
		logger.Error("mqtt client not connected")
		m.client = paho.NewClient(paho.NewClientOptions().AddBroker(m.broker).SetClientID(m.clientId).SetUsername(m.username).SetPassword(m.password))
		if token := m.client.Connect(); token.Wait() && token.Error() != nil {
			logger.Error("reconnect mqtt broker failed, err: " + token.Error().Error())
			return token.Error()
		}
	}
	return nil
}

func (m *mqtt) Subscribe(topic string, qos byte, handlerFunc paho.MessageHandler) error {
	token := m.client.Subscribe(topic, qos, handlerFunc)
	if token.Error() != nil {
		logger.Error("subscribe topic failed, topic: " + topic + ", err: " + token.Error().Error())
		return token.Error()
	}
	token.Wait()
	return nil
}

func (m *mqtt) SubscribeMultiple(filters map[string]byte, callback paho.MessageHandler) error {
	token := m.client.SubscribeMultiple(filters, callback)
	if token.Error() != nil {
		logger.Error("subscribe Topics failed, err: " + token.Error().Error())
		return token.Error()
	}
	token.Wait()
	return nil
}

func (m *mqtt) Publish(topic string, qos byte, retained bool, payload interface{}) error {
	if token := m.client.Publish(topic, qos, retained, payload); token.Wait() && token.Error() != nil {
		logger.Error("publish topic failed, topic: " + topic + ", err: " + token.Error().Error())
		return token.Error()
	}
	return nil
}
func (m *mqtt) UnSubscribe(topics ...string) error {
	if token := m.client.Unsubscribe(topics...); token.Wait() && token.Error() != nil {
		logger.Error("unsubscribe Topics failed, err: " + token.Error().Error())
		return token.Error()
	}
	return nil
}
