import { Context, Schema } from 'koishi'

import Broker, { ListenerFunc } from 'koishi-service-broker'
import amqp, { Channel, ChannelWrapper } from 'amqp-connection-manager'
import { AmqpConnectionManagerOptions, IAmqpConnectionManager } from 'amqp-connection-manager/dist/types/AmqpConnectionManager'
import { ConsumeMessage, Options } from 'amqplib'
import { ConsumerOptions, PublishOptions } from 'amqp-connection-manager/dist/types/ChannelWrapper'



class AmqpBroker extends Broker {

  private readonly connection: IAmqpConnectionManager
  private readonly config: AmqpBroker.Config
  private readonly channels: Map<string, ChannelWrapper> = new Map()

  constructor(ctx: Context, config: AmqpBroker.Config) {
    super(ctx)
    this.config = config
    this.logger.info('connecting to amqp broker: ', config.urls.join(', '))
    this.connection = amqp.connect(config.urls, { ...config.connect_options} )
    this.connection.on('connect', ({url}) => {
      this.logger.info('amqp broker connected to ', url)
    })
    this.connection.on('connectFailed', ({ err, url }) => {
      this.logger.error(`amqp broker connect failed to ${url}`)
      this.logger.error(err)
    })
    this.connection.on('disconnect', ({ err }) => {
      this.logger.error('amqp broker disconnected')
      this.logger.error(err)
    })
    this.connection.on('blocked', ({ reason }) => {
      this.logger.warn('amqp broker blocked: ', reason)
    })
    this.connection.on('unblocked', () => {
      this.logger.info('amqp broker unblocked')
    })
    ctx.on('dispose', () => {
      this.connection.close().then(() => {
        this.logger.info('amqp broker closed')
      }).catch(err => {
        this.logger.error('amqp broker error while closing')
        this.logger.error(err)
      })
    })
  }

  private accureChannel(topic: string): ChannelWrapper {
    let channel = this.channels.get(topic)
    if (!channel) {
      const config = this.config
      channel = this.connection.createChannel({
        name: `koishi_${topic}`,
        json: config.json,
        confirm: config.enable_ack,
        publishTimeout: config.publish_timeout,
        setup: function (channel: Channel) {
          return Promise.all([
            channel.assertQueue(topic, { ...config.assert_queue_options }),
            channel.assertExchange(config.exchange_name, config.exchange_type, { ...config.assert_change_options }),
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
    json: boolean,
    publish_timeout: number,

    connect_options?: AmqpConnectionManagerOptions,
    assert_change_options?: Options.AssertExchange,
    assert_queue_options?: Options.AssertQueue,
    consume_options?: ConsumerOptions,
    publish_options?: PublishOptions
  }

  export const Config: Schema<Config> = Schema.object({
    urls: Schema.array(Schema.string()).description('AMQP URL地址'),
    enable_ack: Schema.boolean().default(false).description('是否启用ACK机制'),
    exchange_name: Schema.string().default('koishi').description('交换机名称'),
    exchange_type: Schema.union([
      Schema.const('direct'),
      Schema.const('topic'),
      Schema.const('headers'),
      Schema.const('fanout'),
      Schema.const('match'),
    ]).description('交换机类型, 请自行参阅amqp文档').default('topic'),
    json: Schema.boolean().default(false).description('是否使用JSON序列化消息'),
    publish_timeout: Schema.number().default(10000).description('发送消息超时时间'),
  })

}


export default AmqpBroker;
