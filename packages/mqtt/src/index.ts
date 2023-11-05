import { Context, Schema } from 'koishi'
import Broker, { ListenerFunc } from 'koishi-plugin-broker'

class MqttBroker extends Broker {

  constructor(ctx: Context, config: MqttBroker.Config) {
    super(ctx)
  }

  async subscribe(topic: string, callback: ListenerFunc): Promise<void> {
    throw new Error('Method not implemented.')
  }

  async unsubscribe(topic: string): Promise<void> {
    throw new Error('Method not implemented.')
  }

  async publish(topic: string, data: any): Promise<void> {
    throw new Error('Method not implemented.')
  }

  async close(): Promise<void> {
    throw new Error('Method not implemented.')
  }
  
}

namespace MqttBroker { 

  export interface Config {}

  export const Config: Schema<Config> = Schema.object({})

}


export default MqttBroker;