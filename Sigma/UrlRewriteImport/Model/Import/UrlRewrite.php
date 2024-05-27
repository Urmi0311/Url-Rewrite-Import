<?php
namespace Sigma\UrlRewriteImport\Model\Import;

use Exception;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\Json\Helper\Data as JsonHelper;
use Magento\ImportExport\Helper\Data as ImportHelper;
use Magento\ImportExport\Model\Import;
use Magento\ImportExport\Model\Import\Entity\AbstractEntity;
use Magento\ImportExport\Model\Import\ErrorProcessing\ProcessingErrorAggregatorInterface;
use Magento\ImportExport\Model\ResourceModel\Helper;
use Magento\ImportExport\Model\ResourceModel\Import\Data;
use Psr\Log\LoggerInterface;

class UrlRewrite extends AbstractEntity
{
    public const ENTITY_CODE = 'url_rewriteimport';
    public const TABLE = 'url_rewrite';
    public const ENTITY_ID_COLUMN = 'entity_id';

    /**
     * @var bool Whether to check column names
     */
    protected $needColumnCheck = true;

    /**
     * @var bool Whether to log in history
     */
    protected $logInHistory = true;

    /**
     * @var array List of permanent attributes
     */
    protected $_permanentAttributes = [
        'entity_id'
    ];

    /**
     * Logger Interface
     * @var  LoggerInterface
     */
    protected $logger;
    /**
     * List of valid column names
     *
     * @var array
     */
    protected $validColumnNames = [
        'entity_id',
        'request_path',
        'target_path',
        'redirect_type',
        'store_id'
    ];

    /**
     * Database connection
     *
     * @var AdapterInterface
     */
    protected $connection;

    /**
     * Resource connection
     *
     * @var ResourceConnection
     */
    public $resource;

    /**
     * UrlRewrite constructor.
     *
     * @param JsonHelper $jsonHelper Helper for JSON data
     * @param ImportHelper $importExportData Helper for import/export operations
     * @param Data $importData Data for import operations
     * @param ResourceConnection $resource Database resource connection
     * @param Helper $resourceHelper Resource helper
     * @param ProcessingErrorAggregatorInterface $errorAggregator Error aggregator for import operations
     * @param LoggerInterface $logger Logger
     */
    public function __construct(
        JsonHelper $jsonHelper,
        ImportHelper $importExportData,
        Data $importData,
        ResourceConnection $resource,
        Helper $resourceHelper,
        ProcessingErrorAggregatorInterface $errorAggregator,
        LoggerInterface $logger
    ) {
        $this->jsonHelper = $jsonHelper;
        $this->_importExportData = $importExportData;
        $this->_resourceHelper = $resourceHelper;
        $this->_dataSourceModel = $importData;
        $this->resource = $resource;
        $this->connection = $resource->getConnection(ResourceConnection::DEFAULT_CONNECTION);
        $this->errorAggregator = $errorAggregator;
        $this->logger = $logger;
    }

    /**
     * Get entity type code
     *
     * @return string
     */
    public function getEntityTypeCode()
    {
        return static::ENTITY_CODE;
    }

    /**
     * Get valid column names
     *
     * @return array
     */
    public function getValidColumnNames(): array
    {
        return $this->validColumnNames;
    }

    /**
     * Import data
     *
     * @return bool
     */
    protected function _importData(): bool
    {

        switch ($this->getBehavior()) {
            case Import::BEHAVIOR_DELETE:
                $this->deleteEntity();
                break;
            case Import::BEHAVIOR_REPLACE:
                $this->saveAndReplaceEntity();
                break;
            case Import::BEHAVIOR_APPEND:
                $this->saveAndReplaceEntity();
                break;
        }

        return true;
    }

    /**
     * Delete entity
     *
     * @return bool
     */
    public function deleteEntity(): bool
    {
        $this->logger->info("delete");
        $rows = [];
        while ($bunch = $this->_dataSourceModel->getNextBunch()) {
            foreach ($bunch as $rowNum => $rowData) {
                $this->validateRow($rowData, $rowNum);

                if (!$this->getErrorAggregator()->isRowInvalid($rowNum)) {
                    $rowId = $rowData[static::ENTITY_ID_COLUMN];
                    $rows[] = $rowId;
                    $this->logger->info('Deleting entity', ['entity_id' => $rowId]); // Log the entity ID
                }

                if ($this->getErrorAggregator()->hasToBeTerminated()) {
                    $this->getErrorAggregator()->addRowToSkip($rowNum);
                }
            }
        }

        if ($rows) {
            return $this->deleteEntityFinish(array_unique($rows));
        }

        return false;
    }

    /**
     * Save and replace data
     */
    public function saveAndReplaceEntity()
    {

        $this->logger->info("save");
        $behavior = $this->getBehavior();
        $rows = [];
        while ($bunch = $this->_dataSourceModel->getNextBunch()) {
            $entityList = [];

            foreach ($bunch as $rowNum => $row) {
                if (!$this->validateRow($row, $rowNum)) {
                    continue;
                }

                if ($this->getErrorAggregator()->hasToBeTerminated()) {
                    $this->getErrorAggregator()->addRowToSkip($rowNum);
                    continue;
                }

                $rowId = $row[static::ENTITY_ID_COLUMN];
                $rows[] = $rowId;
                $columnValues = [];

                foreach ($this->getAvailableColumns() as $columnKey) {
                    $columnValues[$columnKey] = $row[$columnKey];
                }

                $entityList[$rowId][] = $columnValues;
                $this->countItemsCreated += (int) !isset($row[static::ENTITY_ID_COLUMN]);
                $this->countItemsUpdated += (int) isset($row[static::ENTITY_ID_COLUMN]);
                $this->logger->info('Saving entity', $columnValues); // Log the column values
            }

            if (Import::BEHAVIOR_REPLACE === $behavior) {
                if ($rows && $this->deleteEntityFinish(array_unique($rows))) {
                    $this->saveEntityFinish($entityList);
                }
            } elseif (Import::BEHAVIOR_APPEND === $behavior) {
                $this->saveEntityFinish($entityList);
            }
        }
    }

    /**
     * Save entity.
     *
     * @param array $entityData The data of the entities to save
     * @return bool
     */
    public function saveEntityFinish(array $entityData): bool
    {
        if ($entityData) {
            $tableName = $this->connection->getTableName(static::TABLE);
            $rows = [];

            foreach ($entityData as $entityRows) {
                foreach ($entityRows as $row) {
                    $rows[] = $row;
                }
            }

            if ($rows) {
                $this->connection->insertOnDuplicate($tableName, $rows, $this->getAvailableColumns());
                return true;
            }

            return false;
        }
    }

    /**
     * Deletes entities from the database based on provided entity IDs.
     *
     * @param array $entityIds The IDs of the entities to delete.
     * @return bool
     */
    public function deleteEntityFinish(array $entityIds): bool
    {
        if ($entityIds) {
            try {
                $this->countItemsDeleted += $this->connection->delete(
                    $this->connection->getTableName(static::TABLE),
                    $this->connection->quoteInto(static::ENTITY_ID_COLUMN . ' IN (?)', $entityIds)
                );
                return true;
            } catch (Exception $e) {
                return false;
            }
        }

        return false;
    }

    /**
     * Retrieve the available columns for the import operation.
     *
     * @return array The available columns for the import.
     */
    public function getAvailableColumns(): array
    {
        return $this->validColumnNames;
    }

    /**
     * Validate a row of data for import.
     *
     * @param array $rowData The data of the row to validate.
     * @param int $rowNum The row number being validated.
     * @return bool True if the row is valid, false otherwise.
     */
    public function validateRow(array $rowData, $rowNum): bool
    {
        $this->logger->info("validate");
        $entityId = $rowData['entity_id'] ?? '';
        $requestPath = $rowData['request_path'] ?? '';
        $targetPath = $rowData['target_path'] ?? '';
        $redirectType = $rowData['redirect_type'] ?? '';
        $storeId = $rowData['store_id'] ?? '';

        if (!$entityId) {
            $this->logger->info('Entity ID is required', ['row_num' => $rowNum]);
            $this->addRowError('EntityIdIsRequired', $rowNum);
        }

        if (!$requestPath) {
            $this->logger->info('RequestPathIsRequired', ['row_num' => $rowNum]);
            $this->addRowError('RequestPathIsRequired', $rowNum);
        }

        if (!$targetPath) {
            $this->logger->info('TargetPathIsRequired', ['row_num' => $rowNum]);
            $this->addRowError('TargetPathIsRequired', $rowNum);
        }

        if (!$storeId) {
            $this->logger->info('StoreIdIsRequired', ['row_num' => $rowNum]);
            $this->addRowError('StoreIdIsRequired', $rowNum);
        }

        if ($redirectType === '' || !in_array((string)$redirectType, ['0', '301', '302'], true)) {
            $this->logger->info('RedirectTypeIsRequired', ['row_num' => $rowNum]);
            $this->addRowError('RedirectTypeIsRequired', $rowNum);
        }

        if (isset($this->_validatedRows[$rowNum])) {
            return !$this->getErrorAggregator()->isRowInvalid($rowNum);
        }

        $this->_validatedRows[$rowNum] = true;

        return !$this->getErrorAggregator()->isRowInvalid($rowNum);
    }

    /**
     * Initialize message templates for validation errors.
     */
    public function initMessageTemplates()
    {
        $this->addMessageTemplate(
            'EntityTypeIsRequired',
            __('The entity type cannot be empty.')
        );
        $this->addMessageTemplate(
            'EntityIdIsRequired',
            __('The entity ID cannot be empty.')
        );
        $this->addMessageTemplate(
            'RequestPathIsRequired',
            __('The request path cannot be empty.')
        );
        $this->addMessageTemplate(
            'TargetPathIsRequired',
            __('The target path cannot be empty.')
        );
        $this->addMessageTemplate(
            'RedirectTypeIsRequired',
            __('The redirect type cannot be empty.')
        );
        $this->addMessageTemplate(
            'StoreIdIsRequired',
            __('The store ID cannot be empty.')
        );
    }
}
