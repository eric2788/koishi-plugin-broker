# koishi-plugin-broker

[![npm](https://img.shields.io/npm/v/koishi-service-broker?style=flat-square)](https://www.npmjs.com/package/koishi-service-broker)

基于 broker 的订阅发布服务

## 插件

- [koishi-plugin-broker-mqtt](https://github.com/eric2788/koishi-plugin-broker/tree/master/packages/mqtt)
- [koishi-plugin-broker-amqp](https://github.com/eric2788/koishi-plugin-broker/tree/master/packages/amqp)
- [koishi-plugin-broker-redis](https://github.com/eric2788/koishi-plugin-broker/tree/master/packages/redis)

## 使用

```ts
import { Context, Schema } from 'koishi'
import 'koishi-service-broker'

export const name = 'test-broker'

export const inject = ['broker']

export interface Config {}

export const Config: Schema<Config> = Schema.object({})

export function apply(ctx: Context) {
    
  // subscribe
  ctx.broker.subscribe('koishi:test', (data: any, properties: any) => {
    // your logic
  })

  // publish
  ctx.broker.publish('koishi:test', 'foobar')
}

```

## 创建你的自定义服务


```ts
import { Context, Schema } from 'koishi'
import Broker, { ListenerFunc } from 'koishi-service-broker'

class YourCustomBroker extends Broker {


  constructor(ctx: Context, config: MqttBroker.Config) {
    super(ctx)
    // ... your startup logic
  }

  async subscribe(topic: string, callback: ListenerFunc): Promise<unknown> {
    // ... your subscribe logic
  }

  async unsubscribe(topic: string): Promise<unknown> {
     // ... your unsubscribe logic
  }

  async publish(topic: string, data: any): Promise<unknown> {
     // ... your publish logic
  }

  async close(): Promise<void> {
     // ... your close logic
  }
  
}

namespace YourCustomBroker { 

  export interface Config {
    // your config
  }

  export const Config: Schema<Config> = Schema.object({
    // your config schema
  })

}


export default YourCustomBroker;
```