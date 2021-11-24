<?php
declare(strict_types=1);
namespace Flownative\JobQueue\CommandJobs;

/*
 * This file is part of the Flownative.JobQueue.CommandJobs package.
 *
 * (c) Flownative GmbH - www.flownative.com
 */

use Flowpack\JobQueue\Common\Job\JobInterface;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\Flow\Annotations as Flow;

/**
 * Job which is based on a Command
 */
class CommandJob implements JobInterface
{
    /**
     * @Flow\Inject
     * @var CommandJobDispatcher
     */
    protected $commandJobDispatcher;

    /**
     * @var string
     */
    protected $commandType;

    /**
     * @var array
     */
    protected $payload;

    /**
     * TopicBasedJob constructor.
     *
     * @param string $commandType
     * @param array $payload
     * @throws InvalidPayloadException
     */
    public function __construct(string $commandType, array $payload)
    {
        $this->commandType = $commandType;
        try {
            $this->payload = json_encode($payload, JSON_THROW_ON_ERROR, 512);
        } catch (\JsonException $e) {
            throw new InvalidPayloadException(sprintf('The payload could not be serialized to JSON: %s', $e->getMessage()), 1637745901, $e);
        }
    }

    /**
     * @param QueueInterface $queue
     * @param Message $message
     * @return bool
     */
    public function execute(QueueInterface $queue, Message $message): bool
    {
        return $this->commandJobDispatcher->dispatch($queue, $this);
    }

    /**
     * @return string
     */
    public function getLabel(): string
    {
        return sprintf('CommandJob (%s)', $this->commandType);
    }

    /**
     * @return string
     */
    public function getCommandType(): string
    {
        return $this->commandType;
    }

    /**
     * @return array
     * @throws InvalidPayloadException
     */
    public function getPayload(): array
    {
        try {
            return json_decode($this->payload, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new InvalidPayloadException(sprintf('The payload could not be deserialized from JSON: %s', $e->getMessage()), 1637745941, $e);
        }
    }
}
