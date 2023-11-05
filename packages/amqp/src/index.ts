import { Context, Schema } from 'koishi'

import Broker, { ListenerFunc } from 'koishi-plugin-broker'
import amqp, { Channel, ChannelWrapper } from 'amqp-connection-manager'
import { IAmqpConnectionManager } from 'amqp-connection-manager/dist/types/AmqpConnectionManager'
import { ConsumeMessage } from 'amqplib'



class AmqpBroker extends Broker {

  private readonly connection: IAmqpConnectionManager
  private readonly enableAck: boolean
  private readonly channels: Map<string, ChannelWrapper>

  constructor(ctx: Context, config: AmqpBroker.Config) {
    super(ctx)
    this.enableAck = config.enable_ack
    this.connection = amqp.connect(config.urls)
  }

  private accureChannel(topic: string): ChannelWrapper {
    let channel = this.channels.get(topic)
    if (!channel) {
      channel = this.connection.createChannel({
        json: true,
        confirm: this.enableAck,
        setup: function (channel: Channel) {
          return Promise.all([
            channel.assertQueue(topic, { durable: true }),
            channel.assertExchange('koishi', 'topic', { durable: true }),
          ]);
        },
      })
      this.channels.set(topic, channel)
    }
    return channel
  }

  async subscribe(topic: string, callback: ListenerFunc): Promise<void> {
    const channelWrapper = this.accureChannel(topic)
    channelWrapper.consume(topic, (msg: ConsumeMessage) => callback(msg.content, msg))
  }

  async unsubscribe(topic: string): Promise<void> {
    const channelWrapper = this.channels.get(topic)
    if (!channelWrapper) return
    return channelWrapper.cancel(topic)
      .then(() => {
        this.channels.delete(topic)
      })
  }

  async publish(topic: string, data: any): Promise<void> {
    const channelWrapper = this.accureChannel(topic)
    return channelWrapper.sendToQueue(topic, data).then(done => console.debug('sent: ', done, 'data: ', data))
  }

  async close(): Promise<void> {
    return this.connection.close()
  }

}


namespace AmqpBroker {

  export interface Config {
    urls: string[],
    enable_ack: boolean,
  }

  export const Config: Schema<Config> = Schema.object({
    urls: Schema.array(Schema.string()).description('AMQP URL地址'),
    enable_ack: Schema.boolean().default(false).description('是否启用ACK机制'),
  })

}


export default AmqpBroker;
