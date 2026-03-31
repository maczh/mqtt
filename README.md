# MGin MQTT注册插件

## 安装
```bash
go get -u github.com/maczh/mqtt
```

## 使用
在MGin微服务模块的main.go中,在app := mgin.NewApp()之后，加入一行

```go
	//加载MQTT消息队列
    app.MGin.UsePlugin("mqtt", mqtt.MQTT)
```

## yml配置
### 在MGin微服务模块本地配置文件中
```yaml
go:
  config:
    used: mqtt
    prefix:
      mqtt: mqtt
```

### 配置中心的mqtt-test.yml配置,单连接
```yaml
go:
  data:
    mqtt:
      multi: false
      broker: tcp://172.30.226.52:1883
      clientId: go-mqtt-client
      username:
      password:

```
### 配置中心的mqtt-test.yml配置,多连接
```yaml
go:
  data:
    mqtt:
      multi: true
      conns: broker1, broker2
      broker1:
        broker: tcp://172.30.226.52:1883
        clientId: go-mqtt-client1
        username:
        password:
      broker2:
        broker: tcp://172.30.226.53:1883
        clientId: go-mqtt-client2
        username:
        password:
```


## 发送消息
```go
    mqtt.MQTT.Publish("","mytopic", 0, false, msg)
```

## 侦听主题消息并处理

- 定义消息处理函数
```go
import (
    paho "github.com/eclipse/paho.mqtt.golang"
    "github.com/maczh/mgin"
    "github.com/maczh/mqtt"
)

var testHandler paho.MessageHandler = func(client paho.Client, msg paho.Message) {
    fmt.Printf("Received topic: %s\n%s\n", msg.Topic(), string(msg.Payload()))
}

```

- 在main.go中添加侦听代码
```go
	//侦听MQTT消息，说明，主题名后缀的#为通配符，代表可以侦听同一前缀的所有主题消息
    mqtt.MQTT.Subscribe("","test/topic/#", 0, testHandler)
```

## 版本升级说明
- v1.0.0 2025-09-25 初始版本
- v1.0.1 2026-02-04 修复订阅协程崩溃问题
- v1.1.0 2026-04-01 支持多连接