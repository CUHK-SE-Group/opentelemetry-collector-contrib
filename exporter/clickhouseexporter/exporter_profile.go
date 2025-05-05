// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clickhouseexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // For register database driver.
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter/internal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
)

type profilesExporter struct {
	client           *sql.DB
	insertProfileSQL string
	insertSampleSQL  string
	insertFrameSQL   string
	logger           *zap.Logger
	cfg              *Config
}

// batchData represents collected data for insertion
type profileBatch struct {
	timestamps                 []time.Time
	profileIDs                 []string
	serviceNames               []string
	scopeNames                 []string
	scopeVersions              []string
	resourceAttributes         []column.IterableOrderedMap
	scopeAttributes            []column.IterableOrderedMap
	schemaURLs                 []string
	scopeSchemaURLs            []string
	durations                  []int64
	periodTypeNames            []string
	periodTypeUnits            []string
	periodTypeAggTemporalities []int32
	periods                    []int64
	defaultSampleTypes         []string
	comments                   [][]string
	droppedAttributesCounts    []uint32
	originalPayloadFormats     []string
	originalPayloads           []string
	profileAttributes          []column.IterableOrderedMap
}

type sampleBatch struct {
	profileIDs             []string
	traceIDs               []string
	spanIDs                []string
	sampleTypes            []string
	sampleUnits            []string
	aggregationTemporality []int32
	values                 [][]int64
	timestamps             [][]time.Time
	locationsStartIndices  []int32
	locationsLengths       []int32
	depths                 []uint8
	attributes             []column.IterableOrderedMap
}

type frameBatch struct {
	profileIDs             []string
	locationIndices        []int32
	mappingIndices         []int32
	addresses              []uint64
	isFoldeds              []uint8
	locationAttributes     []column.IterableOrderedMap
	lineIndices            []int32
	functionIndices        []int32
	functionNames          []string
	systemNames            []string
	filenames              []string
	startLines             []int64
	lines                  []int64
	columns                []int64
	mappingMemoryStarts    []uint64
	mappingMemoryLimits    []uint64
	mappingFileOffsets     []uint64
	mappingFilenames       []string
	mappingHasFunctions    []uint8
	mappingHasFilenames    []uint8
	mappingHasLineNumbers  []uint8
	mappingHasInlineFrames []uint8
	mappingAttributes      []column.IterableOrderedMap
}

func newProfilesExporter(logger *zap.Logger, cfg *Config) (*profilesExporter, error) {
	client, err := newClickhouseClient(cfg)
	if err != nil {
		return nil, err
	}

	return &profilesExporter{
		client:           client,
		insertProfileSQL: renderInsertProfilesSQL(cfg),
		insertSampleSQL:  renderInsertSamplesSQL(cfg),
		insertFrameSQL:   renderInsertFramesSQL(cfg),
		logger:           logger,
		cfg:              cfg,
	}, nil
}

func (e *profilesExporter) start(ctx context.Context, _ component.Host) error {
	if !e.cfg.shouldCreateSchema() {
		return nil
	}

	if err := createDatabase(ctx, e.cfg); err != nil {
		return err
	}

	return createProfileTables(ctx, e.cfg, e.client)
}

// shutdown will shut down the exporter.
func (e *profilesExporter) shutdown(_ context.Context) error {
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func (e *profilesExporter) pushProfileData(ctx context.Context, pd pprofile.Profiles) error {
	start := time.Now()

	// Pre-allocate batch collectors with estimated size
	totalProfiles := countProfiles(pd)
	totalSamples := countSamples(pd)
	totalFrames := estimateFrameCount(pd)

	profileBatch := &profileBatch{
		timestamps:                 make([]time.Time, 0, totalProfiles),
		profileIDs:                 make([]string, 0, totalProfiles),
		serviceNames:               make([]string, 0, totalProfiles),
		scopeNames:                 make([]string, 0, totalProfiles),
		scopeVersions:              make([]string, 0, totalProfiles),
		resourceAttributes:         make([]column.IterableOrderedMap, 0, totalProfiles),
		scopeAttributes:            make([]column.IterableOrderedMap, 0, totalProfiles),
		schemaURLs:                 make([]string, 0, totalProfiles),
		scopeSchemaURLs:            make([]string, 0, totalProfiles),
		durations:                  make([]int64, 0, totalProfiles),
		periodTypeNames:            make([]string, 0, totalProfiles),
		periodTypeUnits:            make([]string, 0, totalProfiles),
		periodTypeAggTemporalities: make([]int32, 0, totalProfiles),
		periods:                    make([]int64, 0, totalProfiles),
		defaultSampleTypes:         make([]string, 0, totalProfiles),
		comments:                   make([][]string, 0, totalProfiles),
		droppedAttributesCounts:    make([]uint32, 0, totalProfiles),
		originalPayloadFormats:     make([]string, 0, totalProfiles),
		originalPayloads:           make([]string, 0, totalProfiles),
		profileAttributes:          make([]column.IterableOrderedMap, 0, totalProfiles),
	}

	sampleBatch := &sampleBatch{
		profileIDs:             make([]string, 0, totalSamples),
		traceIDs:               make([]string, 0, totalSamples),
		spanIDs:                make([]string, 0, totalSamples),
		sampleTypes:            make([]string, 0, totalSamples),
		sampleUnits:            make([]string, 0, totalSamples),
		aggregationTemporality: make([]int32, 0, totalSamples),
		values:                 make([][]int64, 0, totalSamples),
		timestamps:             make([][]time.Time, 0, totalSamples),
		locationsStartIndices:  make([]int32, 0, totalSamples),
		locationsLengths:       make([]int32, 0, totalSamples),
		depths:                 make([]uint8, 0, totalSamples),
		attributes:             make([]column.IterableOrderedMap, 0, totalSamples),
	}

	frameBatch := &frameBatch{
		profileIDs:             make([]string, 0, totalFrames),
		locationIndices:        make([]int32, 0, totalFrames),
		mappingIndices:         make([]int32, 0, totalFrames),
		addresses:              make([]uint64, 0, totalFrames),
		isFoldeds:              make([]uint8, 0, totalFrames),
		locationAttributes:     make([]column.IterableOrderedMap, 0, totalFrames),
		lineIndices:            make([]int32, 0, totalFrames),
		functionIndices:        make([]int32, 0, totalFrames),
		functionNames:          make([]string, 0, totalFrames),
		systemNames:            make([]string, 0, totalFrames),
		filenames:              make([]string, 0, totalFrames),
		startLines:             make([]int64, 0, totalFrames),
		lines:                  make([]int64, 0, totalFrames),
		columns:                make([]int64, 0, totalFrames),
		mappingMemoryStarts:    make([]uint64, 0, totalFrames),
		mappingMemoryLimits:    make([]uint64, 0, totalFrames),
		mappingFileOffsets:     make([]uint64, 0, totalFrames),
		mappingFilenames:       make([]string, 0, totalFrames),
		mappingHasFunctions:    make([]uint8, 0, totalFrames),
		mappingHasFilenames:    make([]uint8, 0, totalFrames),
		mappingHasLineNumbers:  make([]uint8, 0, totalFrames),
		mappingHasInlineFrames: make([]uint8, 0, totalFrames),
		mappingAttributes:      make([]column.IterableOrderedMap, 0, totalFrames),
	}

	// Process the profiles and collect data for batch insertion
	for i := 0; i < pd.ResourceProfiles().Len(); i++ {
		resourceProfiles := pd.ResourceProfiles().At(i)
		resource := resourceProfiles.Resource()
		resAttr := internal.AttributesToMap(resource.Attributes())
		serviceName := internal.GetServiceName(resource.Attributes())
		schemaURL := resourceProfiles.SchemaUrl()

		for j := 0; j < resourceProfiles.ScopeProfiles().Len(); j++ {
			scopeProfiles := resourceProfiles.ScopeProfiles().At(j)
			scope := scopeProfiles.Scope()
			scopeSchemaURL := scopeProfiles.SchemaUrl()

			for k := 0; k < scopeProfiles.Profiles().Len(); k++ {
				profile := scopeProfiles.Profiles().At(k)
				profileID := profile.ProfileID().String()
				timeNanos := profile.Time().AsTime().UnixNano()
				durationNanos := profile.Duration().AsTime().UnixNano()
				stringTable := profile.StringTable()

				// Get period type information
				periodTypeName := getString(stringTable, int(profile.PeriodType().TypeStrindex()))
				periodTypeUnit := getString(stringTable, int(profile.PeriodType().UnitStrindex()))
				periodTypeAggregationTemporality := int32(profile.PeriodType().AggregationTemporality())
				defaultSampleType := getString(stringTable, int(profile.DefaultSampleTypeStrindex()))

				// Get comments
				var comments []string
				commentStrindices := profile.CommentStrindices()
				for c := 0; c < commentStrindices.Len(); c++ {
					comments = append(comments, getString(stringTable, int(commentStrindices.At(c))))
				}

				// Original payload handling
				originalPayloadFormat := profile.OriginalPayloadFormat()
				originalPayload := ""
				if profile.OriginalPayload().Len() > 0 {
					originalPayload = string(profile.OriginalPayload().AsRaw())
				}

				// Add to profile batch
				profileBatch.timestamps = append(profileBatch.timestamps, time.Unix(0, timeNanos))
				profileBatch.profileIDs = append(profileBatch.profileIDs, profileID)
				profileBatch.serviceNames = append(profileBatch.serviceNames, serviceName)
				profileBatch.scopeNames = append(profileBatch.scopeNames, scope.Name())
				profileBatch.scopeVersions = append(profileBatch.scopeVersions, scope.Version())
				profileBatch.resourceAttributes = append(profileBatch.resourceAttributes, resAttr)
				profileBatch.scopeAttributes = append(profileBatch.scopeAttributes, internal.AttributesToMap(scope.Attributes()))
				profileBatch.schemaURLs = append(profileBatch.schemaURLs, schemaURL)
				profileBatch.scopeSchemaURLs = append(profileBatch.scopeSchemaURLs, scopeSchemaURL)
				profileBatch.durations = append(profileBatch.durations, durationNanos)
				profileBatch.periodTypeNames = append(profileBatch.periodTypeNames, periodTypeName)
				profileBatch.periodTypeUnits = append(profileBatch.periodTypeUnits, periodTypeUnit)
				profileBatch.periodTypeAggTemporalities = append(profileBatch.periodTypeAggTemporalities, periodTypeAggregationTemporality)
				profileBatch.periods = append(profileBatch.periods, profile.Period())
				profileBatch.defaultSampleTypes = append(profileBatch.defaultSampleTypes, defaultSampleType)
				profileBatch.comments = append(profileBatch.comments, comments)
				profileBatch.droppedAttributesCounts = append(profileBatch.droppedAttributesCounts, profile.DroppedAttributesCount())
				profileBatch.originalPayloadFormats = append(profileBatch.originalPayloadFormats, originalPayloadFormat)
				profileBatch.originalPayloads = append(profileBatch.originalPayloads, originalPayload)
				profileBatch.profileAttributes = append(profileBatch.profileAttributes, internal.AttributesToMap(convertAttributeIndicesToMap(profile)))

				// Process sample types
				sampleType := profile.SampleType().At(0)
				sampleTypeName := getString(stringTable, int(sampleType.TypeStrindex()))
				sampleTypeUnit := getString(stringTable, int(sampleType.UnitStrindex()))
				aggregationTemporality := int(sampleType.AggregationTemporality())

				// Process samples
				for s := 0; s < profile.Sample().Len(); s++ {
					sample := profile.Sample().At(s)

					// Get trace and span ID if available
					traceID := ""
					spanID := ""
					if sample.HasLinkIndex() && int(sample.LinkIndex()) < profile.LinkTable().Len() {
						link := profile.LinkTable().At(int(sample.LinkIndex()))
						traceID = traceutil.TraceIDToHexOrEmptyString(link.TraceID())
						spanID = traceutil.SpanIDToHexOrEmptyString(link.SpanID())
					}

					// Collect values and timestamps as arrays
					var values []int64
					var timestamps []time.Time

					// Get timestamps for the sample
					for t := 0; t < sample.TimestampsUnixNano().Len(); t++ {
						timestamps = append(timestamps, time.Unix(0, int64(sample.TimestampsUnixNano().At(t))))
					}

					// Get values for the sample
					for v := 0; v < sample.Value().Len(); v++ {
						values = append(values, sample.Value().At(v))
					}

					// Add to sample batch
					sampleBatch.profileIDs = append(sampleBatch.profileIDs, profileID)
					sampleBatch.traceIDs = append(sampleBatch.traceIDs, traceID)
					sampleBatch.spanIDs = append(sampleBatch.spanIDs, spanID)
					sampleBatch.sampleTypes = append(sampleBatch.sampleTypes, sampleTypeName)
					sampleBatch.sampleUnits = append(sampleBatch.sampleUnits, sampleTypeUnit)
					sampleBatch.aggregationTemporality = append(sampleBatch.aggregationTemporality, int32(aggregationTemporality))
					sampleBatch.values = append(sampleBatch.values, values)
					sampleBatch.timestamps = append(sampleBatch.timestamps, timestamps)
					sampleBatch.locationsStartIndices = append(sampleBatch.locationsStartIndices, sample.LocationsStartIndex())
					sampleBatch.locationsLengths = append(sampleBatch.locationsLengths, sample.LocationsLength())
					sampleBatch.depths = append(sampleBatch.depths, uint8(s))
					sampleBatch.attributes = append(sampleBatch.attributes, internal.AttributesToMap(convertSampleAttributesToMap(profile, sample)))
				}

				// Process frames (combines locations, functions, and mappings)
				for l := 0; l < profile.LocationTable().Len(); l++ {
					location := profile.LocationTable().At(l)
					mappingIndex := location.MappingIndex()
					address := location.Address()
					locationAttr := internal.AttributesToMap(convertLocationAttributesToMap(profile, location))

					// Get mapping information if available
					var mappingMemoryStart, mappingMemoryLimit, mappingFileOffset uint64
					var mappingFilename string
					var mappingHasFunctions, mappingHasFilenames, mappingHasLineNumbers, mappingHasInlineFrames bool
					var mappingAttr column.IterableOrderedMap

					if mappingIndex >= 0 && int(mappingIndex) < profile.MappingTable().Len() {
						mapping := profile.MappingTable().At(int(mappingIndex))
						mappingMemoryStart = mapping.MemoryStart()
						mappingMemoryLimit = mapping.MemoryLimit()
						mappingFileOffset = mapping.FileOffset()
						mappingFilename = getString(stringTable, int(mapping.FilenameStrindex()))
						mappingHasFunctions = mapping.HasFunctions()
						mappingHasFilenames = mapping.HasFilenames()
						mappingHasLineNumbers = mapping.HasLineNumbers()
						mappingHasInlineFrames = mapping.HasInlineFrames()
						mappingAttr = internal.AttributesToMap(convertMappingAttributesToMap(profile, mapping))
					}

					// If location has no lines, create one row with empty function info
					if location.Line().Len() == 0 {
						frameBatch.profileIDs = append(frameBatch.profileIDs, profileID)
						frameBatch.locationIndices = append(frameBatch.locationIndices, int32(l))
						frameBatch.mappingIndices = append(frameBatch.mappingIndices, mappingIndex)
						frameBatch.addresses = append(frameBatch.addresses, address)
						frameBatch.isFoldeds = append(frameBatch.isFoldeds, boolToUint8(location.IsFolded()))
						frameBatch.locationAttributes = append(frameBatch.locationAttributes, locationAttr)
						frameBatch.lineIndices = append(frameBatch.lineIndices, -1)
						frameBatch.functionIndices = append(frameBatch.functionIndices, -1)
						frameBatch.functionNames = append(frameBatch.functionNames, "")
						frameBatch.systemNames = append(frameBatch.systemNames, "")
						frameBatch.filenames = append(frameBatch.filenames, "")
						frameBatch.startLines = append(frameBatch.startLines, -1)
						frameBatch.lines = append(frameBatch.lines, -1)
						frameBatch.columns = append(frameBatch.columns, -1)
						frameBatch.mappingMemoryStarts = append(frameBatch.mappingMemoryStarts, mappingMemoryStart)
						frameBatch.mappingMemoryLimits = append(frameBatch.mappingMemoryLimits, mappingMemoryLimit)
						frameBatch.mappingFileOffsets = append(frameBatch.mappingFileOffsets, mappingFileOffset)
						frameBatch.mappingFilenames = append(frameBatch.mappingFilenames, mappingFilename)
						frameBatch.mappingHasFunctions = append(frameBatch.mappingHasFunctions, boolToUint8(mappingHasFunctions))
						frameBatch.mappingHasFilenames = append(frameBatch.mappingHasFilenames, boolToUint8(mappingHasFilenames))
						frameBatch.mappingHasLineNumbers = append(frameBatch.mappingHasLineNumbers, boolToUint8(mappingHasLineNumbers))
						frameBatch.mappingHasInlineFrames = append(frameBatch.mappingHasInlineFrames, boolToUint8(mappingHasInlineFrames))
						frameBatch.mappingAttributes = append(frameBatch.mappingAttributes, mappingAttr)
						continue
					}

					// Process each line for this location
					for lineIdx := 0; lineIdx < location.Line().Len(); lineIdx++ {
						line := location.Line().At(lineIdx)
						functionIndex := line.FunctionIndex()

						// Default values for function info
						functionName := ""
						systemName := ""
						filename := ""
						var startLine int64 = -1

						// Get function info if available
						if functionIndex >= 0 && functionIndex < int32(profile.FunctionTable().Len()) {
							function := profile.FunctionTable().At(int(functionIndex))
							functionName = getString(stringTable, int(function.NameStrindex()))
							systemName = getString(stringTable, int(function.SystemNameStrindex()))
							filename = getString(stringTable, int(function.FilenameStrindex()))
							startLine = function.StartLine()
						}

						frameBatch.profileIDs = append(frameBatch.profileIDs, profileID)
						frameBatch.locationIndices = append(frameBatch.locationIndices, int32(l))
						frameBatch.mappingIndices = append(frameBatch.mappingIndices, mappingIndex)
						frameBatch.addresses = append(frameBatch.addresses, address)
						frameBatch.isFoldeds = append(frameBatch.isFoldeds, boolToUint8(location.IsFolded()))
						frameBatch.locationAttributes = append(frameBatch.locationAttributes, locationAttr)
						frameBatch.lineIndices = append(frameBatch.lineIndices, int32(lineIdx))
						frameBatch.functionIndices = append(frameBatch.functionIndices, functionIndex)
						frameBatch.functionNames = append(frameBatch.functionNames, functionName)
						frameBatch.systemNames = append(frameBatch.systemNames, systemName)
						frameBatch.filenames = append(frameBatch.filenames, filename)
						frameBatch.startLines = append(frameBatch.startLines, startLine)
						frameBatch.lines = append(frameBatch.lines, line.Line())
						frameBatch.columns = append(frameBatch.columns, line.Column())
						frameBatch.mappingMemoryStarts = append(frameBatch.mappingMemoryStarts, mappingMemoryStart)
						frameBatch.mappingMemoryLimits = append(frameBatch.mappingMemoryLimits, mappingMemoryLimit)
						frameBatch.mappingFileOffsets = append(frameBatch.mappingFileOffsets, mappingFileOffset)
						frameBatch.mappingFilenames = append(frameBatch.mappingFilenames, mappingFilename)
						frameBatch.mappingHasFunctions = append(frameBatch.mappingHasFunctions, boolToUint8(mappingHasFunctions))
						frameBatch.mappingHasFilenames = append(frameBatch.mappingHasFilenames, boolToUint8(mappingHasFilenames))
						frameBatch.mappingHasLineNumbers = append(frameBatch.mappingHasLineNumbers, boolToUint8(mappingHasLineNumbers))
						frameBatch.mappingHasInlineFrames = append(frameBatch.mappingHasInlineFrames, boolToUint8(mappingHasInlineFrames))
						frameBatch.mappingAttributes = append(frameBatch.mappingAttributes, mappingAttr)
					}
				}
			}
		}
	}

	// Flush all batches in parallel for better performance
	var wg sync.WaitGroup
	var errs []error
	var errMu sync.Mutex

	wg.Add(3)

	// Insert profiles in parallel
	go func() {
		defer wg.Done()
		if len(profileBatch.profileIDs) > 0 {
			if err := e.flushProfileBatch(ctx, profileBatch); err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("flush profiles: %w", err))
				errMu.Unlock()
			}
		}
	}()

	// Insert samples in parallel
	go func() {
		defer wg.Done()
		if len(sampleBatch.profileIDs) > 0 {
			if err := e.flushSampleBatch(ctx, sampleBatch); err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("flush samples: %w", err))
				errMu.Unlock()
			}
		}
	}()

	// Insert frames in parallel
	go func() {
		defer wg.Done()
		if len(frameBatch.profileIDs) > 0 {
			if err := e.flushFrameBatch(ctx, frameBatch); err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("flush frames: %w", err))
				errMu.Unlock()
			}
		}
	}()

	wg.Wait()

	if len(errs) > 0 {
		return errs[0]
	}

	duration := time.Since(start)
	e.logger.Debug("insert profiles",
		zap.Int("profile_count", len(profileBatch.profileIDs)),
		zap.Int("sample_count", len(sampleBatch.profileIDs)),
		zap.Int("frame_count", len(frameBatch.profileIDs)),
		zap.String("cost", duration.String()))
	return nil
}

func (e *profilesExporter) flushProfileBatch(ctx context.Context, batch *profileBatch) error {
	if len(batch.profileIDs) == 0 {
		return nil
	}

	return doWithTx(ctx, e.client, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, e.insertProfileSQL)
		if err != nil {
			return fmt.Errorf("prepare profile statement: %w", err)
		}
		defer stmt.Close()

		// Use batch statement for all profiles
		for i := 0; i < len(batch.profileIDs); i++ {
			_, err = stmt.ExecContext(ctx,
				batch.timestamps[i],
				batch.profileIDs[i],
				batch.serviceNames[i],
				batch.scopeNames[i],
				batch.scopeVersions[i],
				batch.resourceAttributes[i],
				batch.scopeAttributes[i],
				batch.schemaURLs[i],
				batch.scopeSchemaURLs[i],
				batch.durations[i],
				batch.periodTypeNames[i],
				batch.periodTypeUnits[i],
				batch.periodTypeAggTemporalities[i],
				batch.periods[i],
				batch.defaultSampleTypes[i],
				batch.comments[i],
				batch.droppedAttributesCounts[i],
				batch.originalPayloadFormats[i],
				batch.originalPayloads[i],
				batch.profileAttributes[i],
			)
			if err != nil {
				return fmt.Errorf("exec profile insert: %w", err)
			}
		}
		return nil
	})
}

func (e *profilesExporter) flushSampleBatch(ctx context.Context, batch *sampleBatch) error {
	if len(batch.profileIDs) == 0 {
		return nil
	}

	return doWithTx(ctx, e.client, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, e.insertSampleSQL)
		if err != nil {
			return fmt.Errorf("prepare sample statement: %w", err)
		}
		defer stmt.Close()

		for i := 0; i < len(batch.profileIDs); i++ {
			_, err = stmt.ExecContext(ctx,
				batch.profileIDs[i],
				batch.traceIDs[i],
				batch.spanIDs[i],
				batch.sampleTypes[i],
				batch.sampleUnits[i],
				batch.aggregationTemporality[i],
				batch.values[i],
				batch.timestamps[i],
				batch.locationsStartIndices[i],
				batch.locationsLengths[i],
				batch.depths[i],
				batch.attributes[i],
			)
			if err != nil {
				return fmt.Errorf("exec sample insert: %w", err)
			}
		}
		return nil
	})
}

func (e *profilesExporter) flushFrameBatch(ctx context.Context, batch *frameBatch) error {
	if len(batch.profileIDs) == 0 {
		return nil
	}

	return doWithTx(ctx, e.client, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, e.insertFrameSQL)
		if err != nil {
			return fmt.Errorf("prepare frame statement: %w", err)
		}
		defer stmt.Close()

		for i := 0; i < len(batch.profileIDs); i++ {
			_, err = stmt.ExecContext(ctx,
				batch.profileIDs[i],
				batch.locationIndices[i],
				batch.mappingIndices[i],
				batch.addresses[i],
				batch.isFoldeds[i],
				batch.locationAttributes[i],
				batch.lineIndices[i],
				batch.functionIndices[i],
				batch.functionNames[i],
				batch.systemNames[i],
				batch.filenames[i],
				batch.startLines[i],
				batch.lines[i],
				batch.columns[i],
				batch.mappingMemoryStarts[i],
				batch.mappingMemoryLimits[i],
				batch.mappingFileOffsets[i],
				batch.mappingFilenames[i],
				batch.mappingHasFunctions[i],
				batch.mappingHasFilenames[i],
				batch.mappingHasLineNumbers[i],
				batch.mappingHasInlineFrames[i],
				batch.mappingAttributes[i],
			)
			if err != nil {
				return fmt.Errorf("exec frame insert: %w", err)
			}
		}
		return nil
	})
}

// Helper function to convert bool to uint8
func boolToUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

// countProfiles returns the total number of profiles across all resources and scopes
func countProfiles(pd pprofile.Profiles) int {
	count := 0
	for i := 0; i < pd.ResourceProfiles().Len(); i++ {
		resourceProfiles := pd.ResourceProfiles().At(i)
		for j := 0; j < resourceProfiles.ScopeProfiles().Len(); j++ {
			scopeProfiles := resourceProfiles.ScopeProfiles().At(j)
			count += scopeProfiles.Profiles().Len()
		}
	}
	return count
}

// countSamples returns the total number of samples across all profiles
func countSamples(pd pprofile.Profiles) int {
	count := 0
	for i := 0; i < pd.ResourceProfiles().Len(); i++ {
		resourceProfiles := pd.ResourceProfiles().At(i)
		for j := 0; j < resourceProfiles.ScopeProfiles().Len(); j++ {
			scopeProfiles := resourceProfiles.ScopeProfiles().At(j)
			for k := 0; k < scopeProfiles.Profiles().Len(); k++ {
				profile := scopeProfiles.Profiles().At(k)
				count += profile.Sample().Len()
			}
		}
	}
	return count
}

// estimateFrameCount estimates the number of frames based on profiles data
func estimateFrameCount(pd pprofile.Profiles) int {
	count := 0
	for i := 0; i < pd.ResourceProfiles().Len(); i++ {
		resourceProfiles := pd.ResourceProfiles().At(i)
		for j := 0; j < resourceProfiles.ScopeProfiles().Len(); j++ {
			scopeProfiles := resourceProfiles.ScopeProfiles().At(j)
			for k := 0; k < scopeProfiles.Profiles().Len(); k++ {
				profile := scopeProfiles.Profiles().At(k)
				// Count locations and their lines
				frameCount := 0
				for l := 0; l < profile.LocationTable().Len(); l++ {
					location := profile.LocationTable().At(l)
					lineCount := location.Line().Len()
					if lineCount == 0 {
						// If no lines, we still create one frame
						frameCount++
					} else {
						frameCount += lineCount
					}
				}
				count += frameCount
			}
		}
	}
	return count
}

func getString(stringTable pcommon.StringSlice, index int) string {
	if index < 0 || index >= stringTable.Len() {
		return ""
	}
	return stringTable.At(index)
}

func convertAttributeIndicesToMap(profile pprofile.Profile) pcommon.Map {
	attrs := pcommon.NewMap()

	for i := 0; i < profile.AttributeIndices().Len(); i++ {
		idx := int(profile.AttributeIndices().At(i))
		if idx >= profile.AttributeTable().Len() {
			continue
		}

		attr := profile.AttributeTable().At(idx)
		attrs.PutStr(attr.Key(), attr.Value().AsString())
	}

	return attrs
}

func convertSampleAttributesToMap(profile pprofile.Profile, sample pprofile.Sample) pcommon.Map {
	attrs := pcommon.NewMap()

	for i := 0; i < sample.AttributeIndices().Len(); i++ {
		idx := int(sample.AttributeIndices().At(i))
		if idx >= profile.AttributeTable().Len() {
			continue
		}

		attr := profile.AttributeTable().At(idx)
		attrs.PutStr(attr.Key(), attr.Value().AsString())
	}

	return attrs
}

func convertLocationAttributesToMap(profile pprofile.Profile, location pprofile.Location) pcommon.Map {
	attrs := pcommon.NewMap()

	for i := 0; i < location.AttributeIndices().Len(); i++ {
		idx := int(location.AttributeIndices().At(i))
		if idx >= profile.AttributeTable().Len() {
			continue
		}

		attr := profile.AttributeTable().At(idx)
		attrs.PutStr(attr.Key(), attr.Value().AsString())
	}

	return attrs
}

func convertMappingAttributesToMap(profile pprofile.Profile, mapping pprofile.Mapping) pcommon.Map {
	attrs := pcommon.NewMap()

	for i := 0; i < mapping.AttributeIndices().Len(); i++ {
		idx := int(mapping.AttributeIndices().At(i))
		if idx >= profile.AttributeTable().Len() {
			continue
		}

		attr := profile.AttributeTable().At(idx)
		attrs.PutStr(attr.Key(), attr.Value().AsString())
	}

	return attrs
}

const (
	// language=ClickHouse SQL
	createProfilesTableSQL = `
CREATE TABLE IF NOT EXISTS %s %s (
	Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
	ProfileId String CODEC(ZSTD(1)),
	ServiceName LowCardinality(String) CODEC(ZSTD(1)),
	ScopeName String CODEC(ZSTD(1)),
	ScopeVersion String CODEC(ZSTD(1)),
	ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	SchemaUrl String CODEC(ZSTD(1)),
	ScopeSchemaUrl String CODEC(ZSTD(1)),
	Duration Int64 CODEC(ZSTD(1)),
	PeriodTypeName String CODEC(ZSTD(1)),
	PeriodTypeUnit String CODEC(ZSTD(1)),
	PeriodTypeAggregationTemporality Int32 CODEC(ZSTD(1)),
	Period Int64 CODEC(ZSTD(1)),
	DefaultSampleType String CODEC(ZSTD(1)),
	Comments Array(String) CODEC(ZSTD(1)),
	DroppedAttributesCount UInt32 CODEC(ZSTD(1)),
	OriginalPayloadFormat String CODEC(ZSTD(1)),
	OriginalPayload String CODEC(ZSTD(1)),
	ProfileAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	INDEX idx_profile_id ProfileId TYPE bloom_filter(0.001) GRANULARITY 1,
	INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_profile_attr_key mapKeys(ProfileAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_profile_attr_value mapValues(ProfileAttributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = %s
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, toDateTime(Timestamp))
%s
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`

	// language=ClickHouse SQL
	createSamplesTableSQL = `
CREATE TABLE IF NOT EXISTS %s %s (
	ProfileId String CODEC(ZSTD(1)),
	TraceId String CODEC(ZSTD(1)),
	SpanId String CODEC(ZSTD(1)),
	SampleType LowCardinality(String) CODEC(ZSTD(1)),
	SampleUnit LowCardinality(String) CODEC(ZSTD(1)),
	AggregationTemporality Int32 CODEC(ZSTD(1)),
	Values Array(Int64) CODEC(Delta, ZSTD(1)),
	Timestamps Array(DateTime64(9)) CODEC(Delta, ZSTD(1)),
	LocationsStartIndex Int32 CODEC(Delta, ZSTD(1)),
	LocationsLength Int32 CODEC(Delta, ZSTD(1)),
	Depth UInt8 CODEC(Delta, ZSTD(1)),
	Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	INDEX idx_profile_id ProfileId TYPE bloom_filter(0.001) GRANULARITY 1,
	INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
	INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = %s
PARTITION BY toStartOfMonth(arrayElement(Timestamps, 1))
ORDER BY (ProfileId, SampleType)
%s
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`

	// language=ClickHouse SQL
	createFramesTableSQL = `
CREATE TABLE IF NOT EXISTS %s %s (
	ProfileId String CODEC(ZSTD(1)),
	LocationIndex Int32 CODEC(Delta, ZSTD(1)),
	MappingIndex Int32 CODEC(Delta, ZSTD(1)),
	Address UInt64 CODEC(Delta, ZSTD(1)),
	IsFolded UInt8 CODEC(ZSTD(1)),
	LocationAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	LineIndex Int32 CODEC(Delta, ZSTD(1)),
	FunctionIndex Int32 CODEC(Delta, ZSTD(1)),
	FunctionName String CODEC(ZSTD(1)),
	SystemName String CODEC(ZSTD(1)),
	Filename String CODEC(ZSTD(1)),
	StartLine Int64 CODEC(Delta, ZSTD(1)),
	Line Int64 CODEC(Delta, ZSTD(1)),
	Column Int64 CODEC(Delta, ZSTD(1)),
	MappingMemoryStart UInt64 CODEC(Delta, ZSTD(1)),
	MappingMemoryLimit UInt64 CODEC(Delta, ZSTD(1)),
	MappingFileOffset UInt64 CODEC(Delta, ZSTD(1)),
	MappingFilename String CODEC(ZSTD(1)),
	MappingHasFunctions UInt8 CODEC(ZSTD(1)),
	MappingHasFilenames UInt8 CODEC(ZSTD(1)),
	MappingHasLineNumbers UInt8 CODEC(ZSTD(1)),
	MappingHasInlineFrames UInt8 CODEC(ZSTD(1)),
	MappingAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	INDEX idx_profile_id ProfileId TYPE bloom_filter(0.001) GRANULARITY 1,
	INDEX idx_function_name FunctionName TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_file_name Filename TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_mapping_filename MappingFilename TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = %s
ORDER BY (ProfileId, LocationIndex, LineIndex)
%s
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`

	// language=ClickHouse SQL
	insertProfilesSQLTemplate = `INSERT INTO %s (
                        Timestamp,
                        ProfileId,
                        ServiceName,
                        ScopeName,
                        ScopeVersion,
                        ResourceAttributes,
                        ScopeAttributes,
                        SchemaUrl,
                        ScopeSchemaUrl,
                        Duration,
                        PeriodTypeName,
                        PeriodTypeUnit,
                        PeriodTypeAggregationTemporality,
                        Period,
                        DefaultSampleType,
                        Comments,
                        DroppedAttributesCount,
                        OriginalPayloadFormat,
                        OriginalPayload,
                        ProfileAttributes
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	// language=ClickHouse SQL
	insertSamplesSQLTemplate = `INSERT INTO %s (
                        ProfileId,
                        TraceId,
                        SpanId,
                        SampleType,
                        SampleUnit,
                        AggregationTemporality,
                        Values,
                        Timestamps,
                        LocationsStartIndex,
                        LocationsLength,
                        Depth,
                        Attributes
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	// language=ClickHouse SQL
	insertFramesSQLTemplate = `INSERT INTO %s (
                        ProfileId,
                        LocationIndex,
                        MappingIndex,
                        Address,
                        IsFolded,
                        LocationAttributes,
                        LineIndex,
                        FunctionIndex,
                        FunctionName,
                        SystemName,
                        Filename,
                        StartLine,
                        Line,
                        Column,
                        MappingMemoryStart,
                        MappingMemoryLimit,
                        MappingFileOffset,
                        MappingFilename,
                        MappingHasFunctions,
                        MappingHasFilenames,
                        MappingHasLineNumbers,
                        MappingHasInlineFrames,
                        MappingAttributes
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
)

func createProfileTables(ctx context.Context, cfg *Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateProfilesTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create profiles table sql: %w", err)
	}

	if _, err := db.ExecContext(ctx, renderCreateSamplesTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create samples table sql: %w", err)
	}

	if _, err := db.ExecContext(ctx, renderCreateFramesTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create frames table sql: %w", err)
	}

	return nil
}

func renderInsertProfilesSQL(cfg *Config) string {
	return fmt.Sprintf(strings.ReplaceAll(insertProfilesSQLTemplate, "'", "`"), cfg.ProfilesTables.Profiles)
}

func renderInsertSamplesSQL(cfg *Config) string {
	return fmt.Sprintf(strings.ReplaceAll(insertSamplesSQLTemplate, "'", "`"), cfg.ProfilesTables.Samples)
}

func renderInsertFramesSQL(cfg *Config) string {
	return fmt.Sprintf(strings.ReplaceAll(insertFramesSQLTemplate, "'", "`"), cfg.ProfilesTables.Frames)
}

func renderCreateProfilesTableSQL(cfg *Config) string {
	ttlExpr := generateTTLExpr(cfg.TTL, "toDate(Timestamp)")
	return fmt.Sprintf(createProfilesTableSQL, cfg.ProfilesTables.Profiles, cfg.clusterString(), cfg.tableEngineString(), ttlExpr)
}

func renderCreateSamplesTableSQL(cfg *Config) string {
	ttlExpr := generateTTLExpr(cfg.TTL, "toDate(arrayElement(Timestamps, 1))")
	return fmt.Sprintf(createSamplesTableSQL, cfg.ProfilesTables.Samples, cfg.clusterString(), cfg.tableEngineString(), ttlExpr)
}

func renderCreateFramesTableSQL(cfg *Config) string {
	// For frames table, don't add TTL as it has no timestamp column
	return fmt.Sprintf(createFramesTableSQL, cfg.ProfilesTables.Frames, cfg.clusterString(), cfg.tableEngineString(), "")
}
