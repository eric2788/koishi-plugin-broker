import { Context, Schema } from 'koishi'

import Broker, { ListenerFunc } from 'koishi-service-broker'
import amqp, { Channel, ChannelWrapper } from 'amqp-connection-manager'
import { IAmqpConnectionManager } from 'amqp-connection-manager/dist/types/AmqpConnectionManager'
import { ConsumeMessage, Options } from 'amqplib'
import { ConsumerOptions, PublishOptions } from 'amqp-connection-manager/dist/types/ChannelWrapper'



class AmqpBroker extends Broker {

  private readonly connection: IAmqpConnectionManager
  private readonly config: AmqpBroker.Config
  private readonly channels: Map<string, ChannelWrapper> = new Map()

  constructor(ctx: Context, config: AmqpBroker.Config) {
    super(ctx)
    this.config = config
    this.connection = amqp.connect(config.urls)
    this.connection.on('connect', () => {
      this.logger.info('amqp broker connected')
    })
    this.connection.on('error', (err) => {
      this.logger.error('amqp broker connect failed: ', err)
    })
  }

  private accureChannel(topic: string): ChannelWrapper {
    let channel = this.channels.get(topic)
    if (!channel) {
      const config = this.config
      channel = this.connection.createChannel({
        json: true,
        confirm: config.enable_ack,
        setup: function (channel: Channel) {
          return Promise.all([
            channel.assertQueue(topic, { durable: true, ...config.assert_queue_options }),
            channel.assertExchange(config.exchange_name, config.exchange_type, { durable: true, ...config.assert_change_options }),
          ]);
        },
      })
      this.channels.set(topic, channel)
    }
    return channel
  }

  async subscribe(topic: string, callback: ListenerFunc): Promise<unknown> {
    const channelWrapper = this.accureChannel(topic)
    return channelWrapper.consume(topic, (msg: ConsumeMessage) => callback(msg.content, msg), this.config.consume_options)
  }

  async unsubscribe(topic: string): Promise<unknown> {
    const channelWrapper = this.channels.get(topic)
    if (!channelWrapper) return
    return channelWrapper.cancel(topic)
      .then(() => {
        this.channels.delete(topic)
      })
  }

  async publish(topic: string, data: any): Promise<unknown> {
    const channelWrapper = this.accureChannel(topic)
    return channelWrapper.sendToQueue(topic, data, this.config.publish_options).then(done => this.logger.debug('sent: ', done, 'data: ', data))
  }

  async close(): Promise<void> {
    return this.connection.close()
  }

}


type ExchangeType = 'direct' | 'topic' | 'headers' | 'fanout' | 'match'


namespace AmqpBroker {

  export interface Config {
    urls: string[],
    enable_ack: boolean,
    exchange_name: string,
    exchange_type: ExchangeType,

    assert_change_options?: Options.AssertExchange,
    assert_queue_options?: Options.AssertQueue,
    consume_options?: ConsumerOptions,
    publish_options?: PublishOptions
  }

  export const Config: Schema<Config> = Schema.object({
    urls: Schema.array(Schema.string().role('secret')).description('AMQP URL地址'),
    enable_ack: Schema.boolean().default(false).description('是否启用ACK机制'),
    exchange_name: Schema.string().default('koishi').description('交换机名称'),
    exchange_type: Schema.union([
      Schema.const('direct'),
      Schema.const('topic'),
      Schema.const('headers'),
      Schema.const('fanout'),
      Schema.const('match'),
    ]).description('交换机类型, 请自行参阅amqp文档').default('topic'),
  })

}


export default AmqpBroker;
