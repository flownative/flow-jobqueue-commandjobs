<?php
namespace Flownative\JobQueue\CommandJobs;

/*
 * This file is part of the Flownative.JobQueue.CommandJobs package.
 *
 * (c) Flownative GmbH - www.flownative.com
 */

use Flownative\Sentry\SentryClientTrait;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use Neos\Flow\Property\PropertyMapper;
use Neos\Flow\Property\PropertyMappingConfiguration;
use Psr\Log\LoggerInterface;
use Sentry\Severity;

/**
 * Configurable dispatcher for jobs which run commands
 *
 * @Flow\Scope("singleton")
 */
class CommandJobDispatcher
{
    use SentryClientTrait;

    /**
     * @Flow\Inject
     * @var ObjectManagerInterface
     */
    protected $objectManager;

    /**
     * @Flow\Inject
     * @var PropertyMapper
     */
    protected $propertyMapper;

    /**
     * @Flow\Inject
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @Flow\InjectConfiguration(path="commandJobDispatcher.commandHandlers")
     * @var array
     */
    protected $commandHandlers;

    /**
     * Dispatch the given job from the specified queue.
     *
     * @param QueueInterface $queue
     * @param CommandJob $job
     * @return bool
     */
    public function dispatch(QueueInterface $queue, CommandJob $job): ?bool
    {
        $this->logger->debug(sprintf('CommandJobDispatcher: dispatching job %s', $job->getLabel()));

        $queueName = $queue->getName();
        $commandType = $job->getCommandType();

        if (!isset($this->commandHandlers[$queueName])) {
            $this->logger->error(sprintf('CommandJobDispatcher: failed dispatching command from unknown queue %s', $queueName));
            return false;
        }

        if (!isset($this->commandHandlers[$queueName][$commandType])) {
            $this->logger->error(sprintf('CommandJobDispatcher: failed dispatching unknown command type %s', $commandType));
            return false;
        }

        $commandHandlerClassName = $this->commandHandlers[$queueName][$commandType]['commandHandlerClassName'];
        $commandHandlerMethodName = $this->commandHandlers[$queueName][$commandType]['commandHandlerMethodName'];
        $commandClassName = $this->commandHandlers[$queueName][$commandType]['commandClassName'];

        if (!$this->objectManager->isRegistered($commandHandlerClassName)) {
            $this->logger->error(sprintf('CommandJobDispatcher: failed dispatching command of type %s because the handler class "%s" does not exist.', $commandType, $commandHandlerClassName));
            return false;
        }

        try {
            $propertyMappingConfiguration = new PropertyMappingConfiguration();
            $propertyMappingConfiguration->allowAllProperties();
            $propertyMappingConfiguration->forProperty('*')->allowAllProperties();
            $propertyMappingConfiguration->forProperty('*')->forProperty('*')->allowAllProperties();
            $propertyMappingConfiguration->forProperty('*')->forProperty('*')->forProperty('*')->allowAllProperties();
            $command = $this->propertyMapper->convert($job->getPayload(), $commandClassName, $propertyMappingConfiguration);
        } catch (\Exception $e) {
            $this->logger->error(sprintf('CommandJobDispatcher: failed dispatching command of type %s; an exception occurred during property mapping: %s', $commandType, $e->getMessage()));
            if ($sentryClient = self::getSentryClient()) {
                $sentryClient->captureThrowable($e);
            }
            return false;
        }

        try {
            $commandHandler = $this->objectManager->get($commandHandlerClassName);
            $commandHandler->$commandHandlerMethodName($command);
            return true;
        } catch (\Throwable $e) {
            $this->logger->error(sprintf('CommandJobDispatcher: failed dispatching command of type %s; an exception occurred during execution: %s', $commandType, $e->getMessage()));
            if ($sentryClient = self::getSentryClient()) {
                $sentryClient->captureThrowable($e, [
                    'queue' => $queue->getName(),
                    'job' => $job->getLabel()
                ]);
            }
            return false;
        }
    }
}
