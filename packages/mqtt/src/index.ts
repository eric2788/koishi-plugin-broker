import { Context, Schema } from 'koishi'
import Broker, { ListenerFunc } from 'koishi-plugin-broker'
import mqtt from 'mqtt'

class MqttBroker extends Broker {

  private readonly callbackMap: Map<string, ListenerFunc> = new Map()
  private readonly client: mqtt.MqttClient

  constructor(ctx: Context, config: MqttBroker.Config) {
    super(ctx)
    this.client = mqtt.connect(config.url)
    this.client.on('connect', () => {
      ctx.logger('mqtt').info('mqtt broker connected')
    })
    this.client.on('message', (topic, message) => {
      const callback = this.callbackMap.get(topic)
      if (callback) {
        callback(message, { topic })
      }
    })
  }

  async subscribe(topic: string, callback: ListenerFunc): Promise<unknown> {
    const granted = await this.client.subscribeAsync(topic)
    this.callbackMap.set(topic, callback)
    return granted
  }

  async subscribes(topics: string[], callback: ListenerFunc): Promise<unknown> {
    const granted = await this.client.subscribeAsync(topics)
    topics.map(topic => this.callbackMap.set(topic, callback))
    return granted
  }

  async unsubscribe(topic: string): Promise<unknown> {
    const result = await this.client.unsubscribeAsync(topic)
    this.callbackMap.delete(topic)
    return result
  }

  async publish(topic: string, data: any): Promise<unknown> {
    return this.client.publishAsync(topic, data)
  }

  async close(): Promise<void> {
    return this.client.endAsync()
  }
  
}

namespace MqttBroker { 

  export interface Config {
    url: string,

  }

  export const Config: Schema<Config> = Schema.object({
    url: Schema.string().description('mqtt URL地址'),
  })

}


export default MqttBroker;