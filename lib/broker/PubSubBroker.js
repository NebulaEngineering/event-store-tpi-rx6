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
            const [topic] = await this.pubsubClient.createTopic(this.eventsTopic);
            this.topic = topic;
            this.startMessageListener(topic);
            observer.next(`Event Store PubSub Broker listening: Topic=${this.eventsTopic}, subscriptionName=${this.eventsTopicSubscription}`);
            observer.complete();
        });
    }

    /**
     * Disconnect the broker and return an observable that completes when disconnected
     */
    stop$() {
        return Rx.Observable.create(observer => {
            this.getSubscription$().subscribe(
                (subscription) => {
                    subscription.removeListener(`message`, this.onMessage);
                    observer.next(`Event Store PubSub Broker removed listener: Topic=${this.eventsTopic}, subscriptionName=${subscription}`);
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
    getSubscription$(topic) {
        return Rx.defer(() => (topic || this.topic).createSubscription(this.eventsTopicSubscription)).pipe(
            map(([subscription]) => subscription),
        );
    }

    /**
     * Starts to listen messages
     */
    startMessageListener(topic) {
        this.messageListenerSubscription = this.getSubscription$(topic)
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