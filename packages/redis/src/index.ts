import { Context, Schema } from 'koishi'
import Broker, { ListenerFunc } from 'koishi-service-broker'
import { createClient, RedisClientOptions, RedisClientType, RedisFunctions, RedisModules, RedisScripts } from 'redis'

class RedisBroker extends Broker {

  private readonly client: RedisClientType<RedisModules, RedisFunctions, RedisScripts>
  private readonly pubClient: RedisClientType<RedisModules, RedisFunctions, RedisScripts>

  constructor(ctx: Context, config: RedisBroker.Config) {
    super(ctx)

    // validate protocol
    if (config.url && !config.url.startsWith('redis')) {
      throw new Error('redis broker url must start with redis://, got '+config.url)
    }

    this.client = createClient(config)
    this.pubClient = this.client.duplicate()
    ctx.on('ready', () => {
      this.client.connect()
        .then(() => {
          this.logger.info('redis broker connected: ', (config.url || 'localhost:6379'))
        }).catch((err) => {
          this.logger.error('redis broker connect failed: ', (config.url || 'localhost:6379'))
          this.logger.error(err)
        })

        this.pubClient.connect()
          .then(() => {
            this.logger.info('redis publisher connected: ', (config.url || 'localhost:6379'))
          }).catch((err) => {
            this.logger.error('redis publisher connect failed: ', (config.url || 'localhost:6379'))
            this.logger.error(err)
          })
    })
    ctx.on('dispose', () => {
      this.client.quit()
        .then(() => {
          this.logger.info('redis broker closed: ', config.url)
        }).catch((err) => {
          this.logger.error('redis broker error while closing: ', config.url)
          this.logger.error(err)
        })
    })
  }

  async subscribe(topic: string, callback: ListenerFunc): Promise<unknown> {
    return this.client.pSubscribe(topic, callback)
  }

  async unsubscribe(topic: string): Promise<unknown> {
    return this.client.pUnsubscribe(topic)
  }

  async publish(topic: string, data: any): Promise<unknown> {
    return this.pubClient.publish(topic, data)
  }

  async close(): Promise<unknown> {
    return this.client.quit()
  }

}



namespace RedisBroker {

  export interface Config extends RedisClientOptions { 
  }

  export const Config: Schema<Config> = Schema.object({
    url: Schema.string().description('redis URL地址'),
    username: Schema.string().description('redis 用户名'),
    password: Schema.string().description('redis 密码'),
    db: Schema.number().description('redis 数据库'),
  })
}

export default RedisBroker;
