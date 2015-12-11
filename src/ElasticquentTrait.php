<?php namespace Elasticquent;

use Carbon\Carbon;
use \Elasticquent\ElasticquentCollection as ElasticquentCollection;
use \Elasticquent\ElasticquentResultCollection as ResultCollection;

/**
 * Elasticquent Trait
 *
 * Functionality extensions for Elequent that
 * makes working with Elasticsearch easier.
 */
trait ElasticquentTrait
{
    /**
     * Uses Timestamps In Index
     *
     * @var bool
     */
    protected $usesTimestampsInIndex = true;

    /**
     * Is ES Document
     *
     * Set to true when our model is
     * populated by a
     *
     * @var bool
     */
    protected $isDocument = false;

    /**
     * Document Score
     *
     * Hit score when using data
     * from Elasticsearch results.
     *
     * @var null|int
     */
    protected $documentScore = null;

    /**
     * Document Version
     *
     * Elasticsearch document version.
     *
     * @var null|int
     */
    protected $documentVersion = null;

    protected $index = null;

    /**
     * Get ElasticSearch Client
     *
     * @return \Elasticsearch\Client
     */
    public function getElasticSearchClient()
    {
        $config = array();

        if (\Config::has('elasticquent.config')) {
            $config = \Config::get('elasticquent.config');
        }

        return \Elasticsearch\ClientBuilder::fromConfig($config, true);
    }

    /**
     * New Collection
     *
     * @param array $models
     * @return Collection
     */
    public function newCollection(array $models = array())
    {
        return new ElasticquentCollection($models);
    }

    /**
     * Get Index Name
     *
     * @return string
     */
    public function getIndexName()
    {
        // The first thing we check is if there
        // is an elasticquery config file and if there is a
        // default index.
        if (\Config::has('elasticquent.default_index')) {
            return \Config::get('elasticquent.default_index');
        }

        // Otherwise we will just go with 'default'
        return 'default';
    }

    /**
     * Get Type Name
     *
     * @return string
     */
    public function getTypeName()
    {
        return $this->getTable();
    }

    /**
     * Uses Timestamps In Index
     *
     * @return void
     */
    public function usesTimestampsInIndex()
    {
        return $this->usesTimestampsInIndex;
    }

    /**
     * Use Timestamps In Index
     *
     * @return void
     */
    public function useTimestampsInIndex()
    {
        $this->usesTimestampsInIndex = true;
    }

    /**
     * Don't Use Timestamps In Index
     *
     * @return void
     */
    public function dontUseTimestampsInIndex()
    {
        $this->usesTimestampsInIndex = false;
    }

    /**
     * Get Mapping Properties
     *
     * @return array
     */
    public function getMappingProperties()
    {
        return $this->mappingProperties;
    }

    /**
     * Set Mapping Properties
     *
     * @param $mapping
     * @internal param array $mappingProperties
     */
    public function setMappingProperties($mapping)
    {
        $this->mappingProperties = $mapping;
    }

    /**
     * Is Elasticsearch Document
     *
     * Is the data in this module sourced
     * from an Elasticsearch document source?
     *
     * @return bool
     */
    public function isDocument()
    {
        return $this->isDocument;
    }

    /**
     * Get Document Score
     *
     * @return null|float
     */
    public function documentScore()
    {
        return $this->documentScore;
    }

    /**
     * Document Version
     *
     * @return null|int
     */
    public function documentVersion()
    {
        return $this->documentVersion;
    }

     /**
     * Get Index Document Data
     *
     * Get the data that Elasticsearch will
     * index for this particular document.
     *
     * @return  array
     */
    public function getIndexDocumentData()
    {
        return $this->toArray();
    }

    public function excludeIndexDocumentData() {
        return [];
    }

    /**
     * Check if indices exist
     *
     * @param $indices
     * @return mixed
     */
    public static function indexExists($indices)
    {
        $instance = new static;

        $params = [
            'index' => $indices
        ];

        return $instance->getElasticSearchClient()->indices()->exists($params);
    }

    /**
     * Index Documents
     *
     * Index all documents in an Eloquent model.
     *
     * @return  array
     */
    public static function addAllToIndex()
    {
        $instance = new static;

        $all = $instance->newQuery()->get(array('*'));

        return $all->addToIndex();
    }

    /**
     * Re-Index All Content
     *
     * @return array
     */
    public static function reindex()
    {
        $instance = new static;

        $all = $instance->newQuery()->get(array('*'));

        return $all->reindex();
    }

    /**
     * Search By Query
     *
     * Search with a query array
     *
     * @param   array $query
     * @param   array $aggregations
     * @param   array $sourceFields
     * @param   int $limit
     * @param   int $offset
     * @return  ResultCollection
     */
    public static function searchByQuery($query = null, $limit = null, $offset = null)
    {
        $instance = new static;

        $params = $instance->getBasicEsParams('read', true, true, $limit, $offset);

        if ($query) {
            $params['body'] = $query;
        } else {
            $params['body']['query'] = ['match_all' => []];
        }

        $result = $instance->getElasticSearchClient()->search($params);
        $result['query'] = json_encode($query);
        return collect($result);
    }

    /**
     * Search
     *
     * Simple search using a match _all query
     *
     * @param   string $term
     * @return  ResultCollection
     */
    public static function search($term = null)
    {
        $instance = new static;

        $params = $instance->getBasicEsParams();

        $params['body']['query']['match']['_all'] = $term;

        $result = $instance->getElasticSearchClient()->search($params);

        return new ResultCollection($result, $instance = new static);
    }

    /**
     * Add to Search Index
     *
     * @throws Exception
     * @return array
     */
    public function addToIndex()
    {
        if ( ! $this->exists) {
            throw new Exception('Document does not exist.');
        }

        $params = $this->getBasicEsParams('write');


        $includeFields = $this->getIndexDocumentData();
        $excludeFields = $this->excludeIndexDocumentData();

        foreach ($excludeFields as $value) {
            if (array_key_exists($value, $includeFields)){
                unset($includeFields[$value]);
            }
        }

        dd($includeFields);

        // Get our document body data.
        $params['body'] = $includeFields;

        // The id for the document must always mirror the
        // key for this model, even if it is set to something
        // other than an auto-incrementing value. That way we
        // can do things like remove the document from
        // the index, or get the document from the index.
        $params['id'] = $this->getKey();

        return $this->getElasticSearchClient()->index($params);
    }

    /**
     * Remove From Search Index
     *
     * @return array
     */
    public function removeFromIndex()
    {
        $params = $this->getBasicEsParams('write');
        return $this->getElasticSearchClient()->delete($params);
    }

    /**
     * Get Search Document
     *
     * Retrieve an ElasticSearch document
     * for this enty.
     *
     * @return array
     */
    public function getIndexedDocument()
    {
        return $this->getElasticSearchClient()->get($this->getBasicEsParams());
    }

    /**
     * Get Basic Elasticsearch Params
     *
     * Most Elasticsearch API calls need the index and
     * type passed in a parameter array.
     *
     * @param     string $indexType
     * @param     bool $getSourceIfPossible
     * @param     bool $getTimestampIfPossible
     * @param     int $limit
     * @param     int $offset
     *
     * @return    array
     */
    public function getBasicEsParams($indexType = 'read', $getSourceIfPossible = false, $getTimestampIfPossible = false, $limit = null, $offset = null)
    {
        $params = array(
            'index'     => $this->getIndexName() . '_' . $indexType,
            'type'      => $this->getTypeName()
        );

        $fieldsParam = array();

        if ($getSourceIfPossible) {
            array_push($fieldsParam, '_source');
        }

        if ($getTimestampIfPossible) {
            array_push($fieldsParam, '_timestamp');
        }

        if ($fieldsParam) {
            $params['fields'] = implode(",", $fieldsParam);
        }

        if (is_numeric($limit)) {
            $params['size'] = $limit;
        }

        if (is_numeric($offset)) {
            $params['from'] = $offset;
        }

        return $params;
    }

    /**
     * Mapping Exists
     *
     * @return bool
     */
    public static function mappingExists()
    {
        $instance = new static;

        $mapping = $instance->getMapping();

        return (empty($mapping)) ? false : true;
    }

    /**
     * Get Mapping
     *
     * @return void
     */
    public static function getMapping()
    {
        $instance = new static;

        $params = $instance->getBasicEsParams();

        return $instance->getElasticSearchClient()->indices()->getMapping($params);
    }

    /**
     * Put Mapping
     *
     * @param    bool $ignoreConflicts
     * @return   array
     */
    public static function putMapping($ignoreConflicts = false)
    {
        $instance = new static;

        $params = array(
            'index'         => $instance->getIndexName() . '_write',
            'type'          => $instance->getTypeName(),
            'body'          => [
                'properties'    => $instance->getMappingProperties()
            ]
        );

        return $instance->getElasticSearchClient()->indices()->putMapping($params);
    }

    /**
     * Rebuild Mapping with zero downtime
     *
     * This will delete and then re-add
     * the mapping for this model.
     *
     * @param bool $seamless
     *
     * @return array
     */
    public static function rebuildIndex()
    {
        $instance = new static;
        $instance->createIndex(false);
        $instance->putMapping();
    }

    /**
     * Create Index
     *
     * @param bool $new
     * @param bool $seamless
     * @param int $shards
     * @param int $replicas
     * @return array
     */
    public static function createIndex($new = true, $shards = 1, $replicas = 0)
    {
        $instance = new static;
        $client = $instance->getElasticSearchClient();
        $timestamp = time();
        $base = $instance->getIndexName();
        $index = [
            'name' => $base . '_' . $timestamp,
            'read' => $base . '_read',
            'write' => $base . '_write'
        ];

        $params = array(
            'index'     => $index['name']
        );


        $params['body']['settings']['number_of_shards'] = $shards;
        $params['body']['settings']['number_of_replicas'] = $replicas;

        $client->indices()->create($params);
        if ($new) {
            $instance->createAlias($index['read'], $index['name']);
            $instance->createAlias($index['write'], $index['name']);
        } else {
            $instance->updateAlias($index['write'], $index['name']);
        }
        $instance->putMapping();
    }


    /**
     * Get the index name from an alias
     *
     * @param $alias
     * @return mixed
     */
    public static function getIndex($alias) {
        $instance = new static;
        $client = $instance->getElasticSearchClient();

        $params = [
            'index' => $alias
        ];

        try {
            $index = collect($client->indices()->get($params))->keys()[0];
        } catch (Exception $exception) {
            $index = '';
        }

        return $index;
    }

    /**
     * Delete Index
     *
     * @param string $index
     * @return array
     */
    public static function deleteIndex($index)
    {
        $instance = new static;

        $client = $instance->getElasticSearchClient();

        $params = [
            'index' => $index
        ];

        return $client->indices()->delete($params);
    }


    /**
     * Create Alias
     *
     * @param $name
     * @param $index
     * @return mixed
     */
    public static function createAlias($name, $index)
    {
        $instance = new static;

        $client = $instance->getElasticSearchClient();

        $params = [
            'index' => $index,
            'name' => $name,
            'body' => [
                'actions' => [
                    [
                        'add' => [
                            'index' => $index,
                            'alias' => $name
                        ]
                    ]
                ]
            ]
        ];

        return $client->indices()->putAlias($params);
    }


    /**
     * Delete Alias
     *
     * @param $name
     * @param $index
     * @return mixed
     */
    public static function deleteAlias($name, $index)
    {
        $instance = new static;
        $client = $instance->getElasticSearchClient();

        $params = [
            'index' => $index,
            'name' => $name
        ];

        return $client->indices()->deleteAlias($params);
    }

    /**
     * Get a list of all aliases
     *
     * @return mixed
     */
    public static function getAliases()
    {
        $instance = new static;
        $client = $instance->getElasticSearchClient();

        return $client->indices()->getAliases();
    }


    /**
     * Update Alias
     *
     * @param $name
     * @param $index
     */
    public static function updateAlias($name, $index) {
        $instance = new static;
        $client = $instance->getElasticSearchClient();

        $old_index = $instance->getIndex($name);
        $instance->deleteAlias($name, $old_index);
        $instance->createAlias($name, $index);
    }

    /**
     * Type Exists
     *
     * Does this type exist?
     *
     * @return bool
     */
    public static function typeExists()
    {
        $instance = new static;

        $params = $instance->getBasicEsParams();

        return $instance->getElasticSearchClient()->indices()->existsType($params);
    }

    /**
     * New From Hit Builder
     *
     * Variation on newFromBuilder. Instead, takes
     *
     * @param  array  $hit
     * @return static
     */
    public function newFromHitBuilder($hit = array())
    {
        $instance = $this->newInstance(array(), true);

        $attributes = $hit['_source'];

        // Add fields to attributes
        if (isset($hit['fields'])) {
            foreach ($hit['fields'] as $key => $value) {
                $attributes[$key] = $value;
            }
        }

        $instance->setRawAttributes((array) $attributes, true);

        // In addition to setting the attributes
        // from the index, we will set the score as well.
        $instance->documentScore = $hit['_score'];

        // This is now a model created
        // from an Elasticsearch document.
        $instance->isDocument = true;

        // Set our document version if it's
        if (isset($hit['_version'])) {
            $instance->documentVersion = $hit['_version'];
        }

        return $instance;
    }
}