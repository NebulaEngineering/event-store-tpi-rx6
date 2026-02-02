'use strict'

const Rx = require('rxjs');
const { filter, map } = require('rxjs/operators');
const uuidv4 = require('uuid/v4');
const { PubSub } = require('@google-cloud/pubsub');


class PubSubBroker {

    constructor({ projectId, eventsTopic, eventsTopicSubscription }) {
        this.eventsTopic = eventsTopic;
        this.eventsTopicSubscription = eventsTopicSubscription;

        this.incomingEvents$ = new Rx.BehaviorSubject(null);
        this.orderedIncomingEvents$ = this.incomingEvents$.pipe(
            filter(msg => msg)
        );
        this.senderId = uuidv4();

        this.pubsubClient = new PubSub(projectId != null ? { projectId } : {});

        this.topic = this.pubsubClient.topic(eventsTopic);
        this.subscription = this.pubsubClient.subscription(eventsTopicSubscription);
    }

    /**
     * Starts broker: ensures topic and subscription exist (creates if needed), then listens for messages.
     * Returns an Observable that resolves to the connection result.
     */
    start$() {
        return Rx.from(
            this._ensureTopicAndSubscription().then(() => {
                this.startMessageListener();
                return `Event Store PubSub Broker listening: Topic=${this.eventsTopic}, subscriptionName=${this.eventsTopicSubscription}`;
            })
        );
    }

    /**
     * Ensures topic and subscription exist (create if missing).
     */
    async _ensureTopicAndSubscription() {
        await this.pubsubClient.createTopic(this.eventsTopic)
        .catch(err => { 
            if (err.code !== 6) {
                throw err;
            }
        });
        await this.pubsubClient
            .topic(this.eventsTopic)
            .createSubscription(this.eventsTopicSubscription)
            .catch(() => null);
    }

    /**
     * Disconnect the broker and return an observable that completes when disconnected.
     */
    stop$() {
        return Rx.Observable.create(observer => {
            if (this.onMessage) {
                this.subscription.removeListener('message', this.onMessage);
                observer.next(`Event Store PubSub Broker removed listener: Topic=${this.eventsTopic}, subscriptionName=${this.eventsTopicSubscription}`);
            }
            if (this.messageListenerSubscription) {
                this.messageListenerSubscription.unsubscribe();
            }
            observer.complete();
        });
        
    }

    /**
     * Publish data through the events topic.
     * Uses topic.publishMessage({ data, attributes }) 
     * @param {Object} data - Payload to publish (will be JSON.stringify'd).
     * @returns {Observable<string>} Message ID.
     */
    publish$(data) {
        const dataBuffer = Buffer.from(JSON.stringify(data));
        return Rx.defer(() =>
            this.topic.publishMessage({
                data: dataBuffer,
                attributes: { senderId: this.senderId }
            })
        );
    }

    /**
     * Returns an Observable that will emit any event related to the given aggregateType.
     * @param {string} aggregateType
     * @param {boolean} ignoreSelfEvents
     */
    getEventListener$(aggregateType, ignoreSelfEvents = true) {
        return this.orderedIncomingEvents$.pipe(
            filter(msg => msg),
            filter(msg => !ignoreSelfEvents || (msg.attributes && msg.attributes.senderId !== this.senderId)),
            map(msg => msg.data),
            filter(evt => evt.at === aggregateType || aggregateType === '*')
        );
    }

    /**
     * Returns an Observable that emits the subscription object
     */
    getSubscription$() {
        return Rx.of(this.subscription);
    }

    /**
     * Starts listening for messages. Uses subscription.on('message', ...) as per nodejs-pubsub listenForMessages.
     */
    startMessageListener() {
        this.onMessage = message => {
            message.ack();
            const raw = typeof message.data === 'string' ? message.data : message.data.toString();
            try {
            this.incomingEvents$.next({
                data: JSON.parse(raw),
                id: message.id,
                    attributes: message.attributes || {},
                    correlationId: message.attributes && message.attributes.correlationId
                });
            } catch (err) {
                console.error('EventStore PubSub invalid message JSON', err.message);
            }
        };

        this.subscription.on('message', this.onMessage);
        this.subscription.on('error', err => {
            console.error('EventStore PubSub subscription error', err);
        });

        this.messageListenerSubscription = Rx.EMPTY.subscribe();
    }
}

module.exports = PubSubBroker;
