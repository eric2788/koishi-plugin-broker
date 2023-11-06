import { Context, Schema, Service } from 'koishi'

declare module 'koishi' {
  interface Context {
    broker: Broker
  }
}


export type ListenerFunc = (data: any, properties: any) => void


abstract class Broker extends Service {

  constructor(ctx: Context) {
    super(ctx, 'broker')
  }

  abstract subscribe(topic: string, callback: ListenerFunc): Promise<unknown>

  async subscribes(topics: string[], callback: ListenerFunc): Promise<unknown> {
    return Promise.all(topics.map(topic => this.subscribe(topic, callback)))
  }

  abstract unsubscribe(topic: string): Promise<unknown>

  async unscribes(topics: string[]): Promise<unknown> {
    return Promise.all(topics.map(topic => this.unsubscribe(topic)))
  }

  abstract publish(topic: string, data: any): Promise<unknown>

  async publishes(topic: string, datas: any[]): Promise<unknown> {
    return Promise.all(datas.map(data => this.publish(topic, data)))
  }

  abstract close(): Promise<unknown>

}

export default Broker;