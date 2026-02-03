'use strict'

const Rx = require('rxjs');
const { filter, map } = require('rxjs/operators');
// Imports the Google Cloud client library
const uuidv4 = require('uuid/v4');
const { PubSub } = require('@google-cloud/pubsub');

class PubSubBroker {

    constructor({ eventsTopic, eventsTopicSubscription }) {
        //this.projectId = projectId;
        this.eventsTopic = eventsTopic;
        this.eventsTopicSubscription = eventsTopicSubscription;
        /**
         * Rx Subject for every incoming event
         */
        this.incomingEvents$ = new Rx.BehaviorSubject();
        this.orderedIncomingEvents$ = this.incomingEvents$.pipe(
            filter(msg => msg)
        )
        this.senderId = uuidv4();
        this.pubsubClient = new PubSub({});
    }

    /**
     * Starts Broker connections
     * Returns an Obserable that resolves to each connection result
     */
    start$() {        
        return new Rx.Observable(async (observer) => {            
            try {
                const [topic] = await this.pubsubClient.createTopic(this.eventsTopic);
                console.log(`@nebulae/event-store-tpi-rx6: Event Store PubSub Broker created topic: Topic=${this.eventsTopic}`);
                this.topic = topic;
            } catch (error) {
                if(error.code === 6) {
                    this.topic = this.pubsubClient.topic(this.eventsTopic);
                    console.log(`@nebulae/event-store-tpi-rx6: Event Store PubSub Broker topic already exists, getting existing topic: Topic=${this.eventsTopic}`);
                } else {
                    observer.error(`@nebulae/event-store-tpi-rx6: Event Store PubSub Broker failed to create or get topic: Topic=${this.eventsTopic}, Error=${error.message}`);
                    return;
                }                
            }
            this.startMessageListener(this.topic);
            observer.next(`@nebulae/event-store-tpi-rx6: Event Store PubSub Broker listening: Topic=${this.eventsTopic}, subscriptionName=${this.eventsTopicSubscription}`);
            observer.complete();
        });
    }

    /**
     * Disconnect the broker and return an observable that completes when disconnected
     */
    stop$() {
        return Rx.Observable.create(observer => {
            Rx.defer(() => this.getSubscription$()).subscribe(
                (subscription) => {
                    subscription.removeListener(`message`, this.onMessage);
                    observer.next(`@nebulae/event-store-tpi-rx6: Event Store PubSub Broker removed listener: Topic=${this.eventsTopic}, subscriptionName=${subscription}`);
                },
                (error) => observer.error(error),
                () => {
                    this.messageListenerSubscription.unsubscribe();
                    observer.complete();
                }
            );

        });

    }

    /**
     * Publish data throught the events topic
     * Returns an Observable that resolves to the sent message ID
     * @param {string} topicName 
     * @param {Object} data 
     */
    publish$(data) {
        const dataBuffer = Buffer.from(JSON.stringify(data));
        return Rx.defer(() =>
            this.topic.publishMessage(
                {
                    data: dataBuffer,
                    attributes: { senderId: this.senderId }
                }
            ));
    }

    /**
     * Returns an Observable that will emit any event related to the given aggregateType
     * @param {string} aggregateType 
     */
    getEventListener$(aggregateType, ignoreSelfEvents = true) {
        return this.orderedIncomingEvents$.pipe(
            filter(msg => msg)
            ,filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId)
            ,map(msg => msg.data)
            ,filter(evt => evt.at === aggregateType || aggregateType == "*")
            )
    }


    /**
     * Returns an Observable that resolves to the subscription
     */
    async getSubscription$(topic) {
        try {
            const [subscription] = await (topic || this.topic).createSubscription(this.eventsTopicSubscription);
            console.log(`@nebulae/event-store-tpi-rx6: Event Store PubSub Broker created subscription: Topic=${this.eventsTopic}, subscriptionName=${this.eventsTopicSubscription}`);
            return subscription;                        
        } catch (error) {
            if(error.code === 6) {
                console.log(`@nebulae/event-store-tpi-rx6: Event Store PubSub Broker subscription already exists, getting existing subscription: Topic=${this.eventsTopic}, subscriptionName=${this.eventsTopicSubscription}`);
                return (topic || this.topic).subscription(this.eventsTopicSubscription);
            } else {
                throw new Error(`@nebulae/event-store-tpi-rx6: Event Store PubSub Broker failed to create or get subscription: Topic=${this.eventsTopic}, subscriptionName=${this.eventsTopicSubscription}, Error=${error.message}`);
            }
        }
    }

    /**
     * Starts to listen messages
     */
    startMessageListener(topic) {
        this.messageListenerSubscription = Rx.defer(() => this.getSubscription$(topic))
            .subscribe(
                (pubSubSubscription) => {
                    this.onMessage = message => {
                        message.ack();
                        this.incomingEvents$.next({
                            data: JSON.parse(message.data),
                            id: message.id,
                            attributes: message.attributes,
                            correlationId: message.attributes.correlationId
                        });
                    };
                    pubSubSubscription.on(`message`, this.onMessage);
                    pubSubSubscription.on('error', error => {
                        console.error('@nebulae/event-store-tpi-rx6.PubSubBroker: Received error:', error);
                    });
                },
                (err) => {
                    console.error('Failed to obtain EventStore subscription', err);
                },
                () => {
                    console.log('GatewayEvents listener has completed!');
                }
            );
    }

}

module.exports = PubSubBroker;