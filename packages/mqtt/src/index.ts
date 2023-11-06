import { Context, Schema } from 'koishi'
import Broker, { ListenerFunc } from 'koishi-service-broker'
import * as mqtt from 'mqtt'

class MqttBroker extends Broker {

  private readonly callbackMap: Map<string, ListenerFunc> = new Map()
  private readonly client: mqtt.MqttClient

  constructor(ctx: Context, config: MqttBroker.Config) {
    super(ctx)
    this.client = mqtt.connect(config.url, { ...config.connect_options })
    this.logger.info('connecting to mqtt broker: ', config.url)
    this.client.on('connect', () => {
      this.logger.info('mqtt broker connected')
    })
    this.client.on('error', (err) => {
      this.logger.error('mqtt broker connect failed: ', err)
    })
    this.client.on('disconnect', (d) => {
      this.logger.error('mqtt broker disconnected: {}', d)
    })
    this.client.on('reconnect', () => {
      this.logger.info('mqtt broker reconnecting...')
    })
    this.client.on('offline', () => {
      this.logger.info('mqtt broker has gone offline')
    })
    this.client.on('message', (topic, message) => {
      const callback = this.callbackMap.get(topic)
      if (callback) {
        callback(message, { topic })
      }
    })
    ctx.on('dispose', () => {
      this.client.endAsync().then(() => {
        this.logger.info('mqtt broker closed')
      }).catch(err => {
        this.logger.error('mqtt broker error while closing')
        this.logger.error(err)
      })
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


    connect_options?: mqtt.IClientOptions,
  }

  export const Config: Schema<Config> = Schema.object({
    url: Schema.string().description('mqtt URL地址'),
  })

}


export default MqttBroker;