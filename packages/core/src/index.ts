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

  abstract subscribe(topic: string, callback: ListenerFunc): Promise<void>

  abstract unsubscribe(topic: string): Promise<void>

  abstract publish(topic: string, data: any): Promise<void>

  abstract close(): Promise<void>

}

export default Broker;