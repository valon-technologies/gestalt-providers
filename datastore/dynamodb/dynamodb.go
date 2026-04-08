package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

const (
	attrPK = "PK"
	attrSK = "SK"

	userPKPrefix     = "USER#"
	emailPKPrefix    = "EMAIL#"
	profileSK        = "PROFILE"
	uniqueEmailSK    = "UNIQUE"
	tokenSKPrefix    = "TOKEN#"
	apiTokenSKPrefix = "APITOKEN#"

	attrID                = "id"
	attrEmail             = "email"
	attrDisplayName       = "display_name"
	attrCreatedAt         = "created_at"
	attrUpdatedAt         = "updated_at"
	attrUserID            = "user_id"
	attrIntegration       = "integration"
	attrConnection        = "connection"
	attrInstance          = "instance"
	attrAccessTokenEnc    = "access_token_encrypted"
	attrRefreshTokenEnc   = "refresh_token_encrypted"
	attrScopes            = "scopes"
	attrExpiresAt         = "expires_at"
	attrLastRefreshedAt   = "last_refreshed_at"
	attrRefreshErrorCount = "refresh_error_count"
	attrMetadataJSON      = "metadata_json"
	attrName              = "name"
	attrHashedToken       = "hashed_token"

	gsiEmail       = "email-index"
	gsiID          = "id-index"
	gsiHashedToken = "hashed-token-index"
)

type Config struct {
	Table    string
	Region   string
	Endpoint string
}

type Store struct {
	client    *dynamodb.Client
	tableName string
}

func NewStore(cfg Config) (*Store, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.Endpoint != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("local", "local", ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: loading aws config: %w", err)
	}

	var clientOpts []func(*dynamodb.Options)
	if cfg.Endpoint != "" {
		clientOpts = append(clientOpts, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	return &Store{
		client:    dynamodb.NewFromConfig(awsCfg, clientOpts...),
		tableName: cfg.Table,
	}, nil
}

func (s *Store) HealthCheck(ctx context.Context) error {
	_, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &s.tableName,
	})
	if err != nil {
		return fmt.Errorf("dynamodb: health check: %w", err)
	}
	return nil
}

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &s.tableName,
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String(attrPK), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String(attrSK), KeyType: ddbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String(attrPK), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrSK), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrEmail), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrID), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrHashedToken), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String(gsiEmail),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String(attrEmail), KeyType: ddbtypes.KeyTypeHash},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			},
			{
				IndexName: aws.String(gsiID),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String(attrID), KeyType: ddbtypes.KeyTypeHash},
					{AttributeName: aws.String(attrSK), KeyType: ddbtypes.KeyTypeRange},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeKeysOnly},
			},
			{
				IndexName: aws.String(gsiHashedToken),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String(attrHashedToken), KeyType: ddbtypes.KeyTypeHash},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	if err != nil {
		var inUse *ddbtypes.ResourceInUseException
		if !errors.As(err, &inUse) {
			return fmt.Errorf("dynamodb: creating table: %w", err)
		}
	}
	return s.waitForTableReady(ctx)
}

func (s *Store) Close() error { return nil }

func (s *Store) GetUser(ctx context.Context, id string) (*gestalt.StoredUser, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.tableName,
		Key:       userKey(id),
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb: getting user: %w", err)
	}
	if out.Item == nil {
		return nil, nil
	}
	return unmarshalUser(out.Item)
}

func (s *Store) FindOrCreateUser(ctx context.Context, email string) (*gestalt.StoredUser, error) {
	user, err := s.queryUserByEmail(ctx, email)
	if err != nil {
		return nil, err
	}
	if user != nil {
		return user, nil
	}

	now := time.Now().UTC().Truncate(time.Second)
	user = &gestalt.StoredUser{
		ID:        uuid.NewString(),
		Email:     email,
		CreatedAt: now,
		UpdatedAt: now,
	}

	cond := expression.AttributeNotExists(expression.Name(attrPK))
	condExpr, err := expression.NewBuilder().WithCondition(cond).Build()
	if err != nil {
		return nil, fmt.Errorf("dynamodb: building condition: %w", err)
	}

	_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []ddbtypes.TransactWriteItem{
			{
				Put: &ddbtypes.Put{
					TableName: &s.tableName,
					Item:      marshalUser(user),
				},
			},
			{
				Put: &ddbtypes.Put{
					TableName: &s.tableName,
					Item: map[string]ddbtypes.AttributeValue{
						attrPK: &ddbtypes.AttributeValueMemberS{Value: emailPKPrefix + email},
						attrSK: &ddbtypes.AttributeValueMemberS{Value: uniqueEmailSK},
						attrID: &ddbtypes.AttributeValueMemberS{Value: user.ID},
					},
					ConditionExpression:       condExpr.Condition(),
					ExpressionAttributeNames:  condExpr.Names(),
					ExpressionAttributeValues: condExpr.Values(),
				},
			},
		},
	})
	if err != nil {
		var txErr *ddbtypes.TransactionCanceledException
		if errors.As(err, &txErr) {
			return s.getUserByEmailRecord(ctx, email)
		}
		return nil, fmt.Errorf("dynamodb: creating user: %w", err)
	}
	return user, nil
}

func (s *Store) PutIntegrationToken(ctx context.Context, token *gestalt.StoredIntegrationToken) error {
	paramsJSON, err := connectionParamsToJSON(token.ConnectionParams)
	if err != nil {
		return fmt.Errorf("dynamodb: encode connection params: %w", err)
	}

	item := marshalIntegrationToken(token, paramsJSON)
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.tableName,
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("dynamodb: storing integration token: %w", err)
	}
	return nil
}

func (s *Store) GetIntegrationToken(ctx context.Context, userID, integration, connection, instance string) (*gestalt.StoredIntegrationToken, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.tableName,
		Key:       tokenKey(userID, integration, connection, instance),
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb: getting integration token: %w", err)
	}
	if out.Item == nil {
		return nil, nil
	}
	return unmarshalIntegrationToken(out.Item)
}

func (s *Store) ListIntegrationTokens(ctx context.Context, userID, integration, connection string) ([]*gestalt.StoredIntegrationToken, error) {
	keyCond := expression.KeyAnd(
		expression.Key(attrPK).Equal(expression.Value(userPKPrefix+userID)),
		expression.KeyBeginsWith(expression.Key(attrSK), tokenSKPrefix),
	)
	builder := expression.NewBuilder().WithKeyCondition(keyCond)
	if integration != "" || connection != "" {
		var filters []expression.ConditionBuilder
		if integration != "" {
			filters = append(filters, expression.Name(attrIntegration).Equal(expression.Value(integration)))
		}
		if connection != "" {
			filters = append(filters, expression.Name(attrConnection).Equal(expression.Value(connection)))
		}
		filter := filters[0]
		for i := 1; i < len(filters); i++ {
			filter = filter.And(filters[i])
		}
		builder = builder.WithFilter(filter)
	}
	expr, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("dynamodb: building list token expression: %w", err)
	}

	var (
		items             []map[string]ddbtypes.AttributeValue
		exclusiveStartKey map[string]ddbtypes.AttributeValue
	)
	for {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &s.tableName,
			KeyConditionExpression:    expr.KeyCondition(),
			FilterExpression:          expr.Filter(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ExclusiveStartKey:         exclusiveStartKey,
		})
		if err != nil {
			return nil, fmt.Errorf("dynamodb: listing integration tokens: %w", err)
		}
		items = append(items, out.Items...)
		if out.LastEvaluatedKey == nil {
			break
		}
		exclusiveStartKey = out.LastEvaluatedKey
	}

	tokens := make([]*gestalt.StoredIntegrationToken, 0, len(items))
	for _, item := range items {
		token, err := unmarshalIntegrationToken(item)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
	}
	return tokens, nil
}

func (s *Store) DeleteIntegrationToken(ctx context.Context, id string) error {
	pk, sk, err := s.lookupKeysByGSI(ctx, gsiID, attrID, id, tokenSKPrefix)
	if err != nil {
		return fmt.Errorf("dynamodb: looking up integration token for delete: %w", err)
	}
	if pk == "" {
		return nil
	}
	_, err = s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &s.tableName,
		Key: map[string]ddbtypes.AttributeValue{
			attrPK: &ddbtypes.AttributeValueMemberS{Value: pk},
			attrSK: &ddbtypes.AttributeValueMemberS{Value: sk},
		},
	})
	if err != nil {
		return fmt.Errorf("dynamodb: deleting integration token: %w", err)
	}
	return nil
}

func (s *Store) PutAPIToken(ctx context.Context, token *gestalt.StoredAPIToken) error {
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.tableName,
		Item:      marshalAPIToken(token),
	})
	if err != nil {
		return fmt.Errorf("dynamodb: storing api token: %w", err)
	}
	return nil
}

func (s *Store) GetAPITokenByHash(ctx context.Context, hashedToken string) (*gestalt.StoredAPIToken, error) {
	keyCond := expression.Key(attrHashedToken).Equal(expression.Value(hashedToken))
	expr, err := expression.NewBuilder().WithKeyCondition(keyCond).Build()
	if err != nil {
		return nil, fmt.Errorf("dynamodb: building api token expression: %w", err)
	}
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 &s.tableName,
		IndexName:                 aws.String(gsiHashedToken),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb: getting api token by hash: %w", err)
	}
	if len(out.Items) == 0 {
		return nil, nil
	}
	token, err := unmarshalAPIToken(out.Items[0])
	if err != nil {
		return nil, err
	}
	if token.ExpiresAt != nil && time.Now().After(*token.ExpiresAt) {
		return nil, nil
	}
	return token, nil
}

func (s *Store) ListAPITokens(ctx context.Context, userID string) ([]*gestalt.StoredAPIToken, error) {
	keyCond := expression.KeyAnd(
		expression.Key(attrPK).Equal(expression.Value(userPKPrefix+userID)),
		expression.KeyBeginsWith(expression.Key(attrSK), apiTokenSKPrefix),
	)
	expr, err := expression.NewBuilder().WithKeyCondition(keyCond).Build()
	if err != nil {
		return nil, fmt.Errorf("dynamodb: building list api token expression: %w", err)
	}

	var (
		items             []map[string]ddbtypes.AttributeValue
		exclusiveStartKey map[string]ddbtypes.AttributeValue
	)
	for {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &s.tableName,
			KeyConditionExpression:    expr.KeyCondition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ExclusiveStartKey:         exclusiveStartKey,
		})
		if err != nil {
			return nil, fmt.Errorf("dynamodb: listing api tokens: %w", err)
		}
		items = append(items, out.Items...)
		if out.LastEvaluatedKey == nil {
			break
		}
		exclusiveStartKey = out.LastEvaluatedKey
	}

	tokens := make([]*gestalt.StoredAPIToken, 0, len(items))
	for _, item := range items {
		token, err := unmarshalAPIToken(item)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
	}
	return tokens, nil
}

func (s *Store) RevokeAPIToken(ctx context.Context, userID, id string) error {
	cond := expression.Name(attrPK).AttributeExists()
	condExpr, err := expression.NewBuilder().WithCondition(cond).Build()
	if err != nil {
		return fmt.Errorf("dynamodb: building revoke condition: %w", err)
	}

	_, err = s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &s.tableName,
		Key: map[string]ddbtypes.AttributeValue{
			attrPK: &ddbtypes.AttributeValueMemberS{Value: userPKPrefix + userID},
			attrSK: &ddbtypes.AttributeValueMemberS{Value: apiTokenSKPrefix + id},
		},
		ConditionExpression:       condExpr.Condition(),
		ExpressionAttributeNames:  condExpr.Names(),
		ExpressionAttributeValues: condExpr.Values(),
	})
	if err != nil {
		var condErr *ddbtypes.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return fmt.Errorf("dynamodb: api token %s for user %s not found", id, userID)
		}
		return fmt.Errorf("dynamodb: revoking api token: %w", err)
	}
	return nil
}

func (s *Store) RevokeAllAPITokens(ctx context.Context, userID string) (int64, error) {
	keyCond := expression.KeyAnd(
		expression.Key(attrPK).Equal(expression.Value(userPKPrefix+userID)),
		expression.KeyBeginsWith(expression.Key(attrSK), apiTokenSKPrefix),
	)
	projection := expression.NamesList(expression.Name(attrPK), expression.Name(attrSK))
	expr, err := expression.NewBuilder().WithKeyCondition(keyCond).WithProjection(projection).Build()
	if err != nil {
		return 0, fmt.Errorf("dynamodb: building revoke-all expression: %w", err)
	}

	var (
		items             []map[string]ddbtypes.AttributeValue
		exclusiveStartKey map[string]ddbtypes.AttributeValue
	)
	for {
		out, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 &s.tableName,
			KeyConditionExpression:    expr.KeyCondition(),
			ProjectionExpression:      expr.Projection(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ExclusiveStartKey:         exclusiveStartKey,
		})
		if err != nil {
			return 0, fmt.Errorf("dynamodb: querying api tokens for revoke-all: %w", err)
		}
		items = append(items, out.Items...)
		if out.LastEvaluatedKey == nil {
			break
		}
		exclusiveStartKey = out.LastEvaluatedKey
	}
	if len(items) == 0 {
		return 0, nil
	}

	const batchSize = 25
	const maxRetries = 3
	var revoked int64
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}

		pending := make([]ddbtypes.WriteRequest, 0, end-i)
		for _, item := range items[i:end] {
			pending = append(pending, ddbtypes.WriteRequest{
				DeleteRequest: &ddbtypes.DeleteRequest{
					Key: map[string]ddbtypes.AttributeValue{
						attrPK: item[attrPK],
						attrSK: item[attrSK],
					},
				},
			})
		}

		for attempt := 0; attempt <= maxRetries && len(pending) > 0; attempt++ {
			out, err := s.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]ddbtypes.WriteRequest{s.tableName: pending},
			})
			if err != nil {
				return revoked, fmt.Errorf("dynamodb: batch deleting api tokens: %w", err)
			}
			unprocessed := out.UnprocessedItems[s.tableName]
			revoked += int64(len(pending) - len(unprocessed))
			pending = unprocessed
		}
		if len(pending) > 0 {
			return revoked, fmt.Errorf("dynamodb: %d api token deletions failed after retries", len(pending))
		}
	}

	return revoked, nil
}

func (s *Store) waitForTableReady(ctx context.Context) error {
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		out, err := s.client.DescribeTable(waitCtx, &dynamodb.DescribeTableInput{
			TableName: &s.tableName,
		})
		if err == nil && tableReady(out.Table) {
			return nil
		}
		if err != nil {
			var notFound *ddbtypes.ResourceNotFoundException
			if !errors.As(err, &notFound) {
				return fmt.Errorf("dynamodb: describing table readiness: %w", err)
			}
		}

		select {
		case <-waitCtx.Done():
			return fmt.Errorf("dynamodb: waiting for table readiness: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func tableReady(table *ddbtypes.TableDescription) bool {
	if table == nil || table.TableStatus != ddbtypes.TableStatusActive {
		return false
	}
	for _, gsi := range table.GlobalSecondaryIndexes {
		if gsi.IndexStatus != ddbtypes.IndexStatusActive {
			return false
		}
	}
	return true
}

func (s *Store) lookupKeysByGSI(ctx context.Context, indexName, keyAttr, keyValue, skPrefix string) (string, string, error) {
	keyCond := expression.KeyAnd(
		expression.Key(keyAttr).Equal(expression.Value(keyValue)),
		expression.KeyBeginsWith(expression.Key(attrSK), skPrefix),
	)
	expr, err := expression.NewBuilder().WithKeyCondition(keyCond).Build()
	if err != nil {
		return "", "", fmt.Errorf("dynamodb: building key lookup expression: %w", err)
	}
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 &s.tableName,
		IndexName:                 aws.String(indexName),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		return "", "", err
	}
	if len(out.Items) == 0 {
		return "", "", nil
	}
	var pk, sk string
	if err := attributevalue.Unmarshal(out.Items[0][attrPK], &pk); err != nil {
		return "", "", fmt.Errorf("dynamodb: unmarshalling pk: %w", err)
	}
	if err := attributevalue.Unmarshal(out.Items[0][attrSK], &sk); err != nil {
		return "", "", fmt.Errorf("dynamodb: unmarshalling sk: %w", err)
	}
	return pk, sk, nil
}

func (s *Store) getUserByEmailRecord(ctx context.Context, email string) (*gestalt.StoredUser, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &s.tableName,
		ConsistentRead: aws.Bool(true),
		Key: map[string]ddbtypes.AttributeValue{
			attrPK: &ddbtypes.AttributeValueMemberS{Value: emailPKPrefix + email},
			attrSK: &ddbtypes.AttributeValueMemberS{Value: uniqueEmailSK},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb: reading email uniqueness record: %w", err)
	}
	if out.Item == nil {
		return nil, fmt.Errorf("dynamodb: email uniqueness record not found after transaction conflict")
	}
	var userID string
	if err := unmarshalString(out.Item, attrID, &userID); err != nil {
		return nil, fmt.Errorf("dynamodb: unmarshalling email record user id: %w", err)
	}
	return s.GetUser(ctx, userID)
}

func (s *Store) queryUserByEmail(ctx context.Context, email string) (*gestalt.StoredUser, error) {
	keyCond := expression.Key(attrEmail).Equal(expression.Value(email))
	expr, err := expression.NewBuilder().WithKeyCondition(keyCond).Build()
	if err != nil {
		return nil, fmt.Errorf("dynamodb: building email query expression: %w", err)
	}
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 &s.tableName,
		IndexName:                 aws.String(gsiEmail),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb: querying user by email: %w", err)
	}
	if len(out.Items) == 0 {
		return nil, nil
	}
	return unmarshalUser(out.Items[0])
}

func userKey(id string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		attrPK: &ddbtypes.AttributeValueMemberS{Value: userPKPrefix + id},
		attrSK: &ddbtypes.AttributeValueMemberS{Value: profileSK},
	}
}

func tokenKey(userID, integration, connection, instance string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		attrPK: &ddbtypes.AttributeValueMemberS{Value: userPKPrefix + userID},
		attrSK: &ddbtypes.AttributeValueMemberS{Value: tokenSKPrefix + integration + "#" + connection + "#" + instance},
	}
}

func marshalUser(user *gestalt.StoredUser) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		attrPK:          &ddbtypes.AttributeValueMemberS{Value: userPKPrefix + user.ID},
		attrSK:          &ddbtypes.AttributeValueMemberS{Value: profileSK},
		attrID:          &ddbtypes.AttributeValueMemberS{Value: user.ID},
		attrEmail:       &ddbtypes.AttributeValueMemberS{Value: user.Email},
		attrDisplayName: &ddbtypes.AttributeValueMemberS{Value: user.DisplayName},
		attrCreatedAt:   &ddbtypes.AttributeValueMemberS{Value: user.CreatedAt.Format(time.RFC3339)},
		attrUpdatedAt:   &ddbtypes.AttributeValueMemberS{Value: user.UpdatedAt.Format(time.RFC3339)},
	}
}

func unmarshalUser(item map[string]ddbtypes.AttributeValue) (*gestalt.StoredUser, error) {
	var (
		user      gestalt.StoredUser
		createdAt string
		updatedAt string
	)
	if err := unmarshalString(item, attrID, &user.ID); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrEmail, &user.Email); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrDisplayName, &user.DisplayName); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrCreatedAt, &createdAt); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrUpdatedAt, &updatedAt); err != nil {
		return nil, err
	}

	var err error
	user.CreatedAt, err = time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: parsing created_at: %w", err)
	}
	user.UpdatedAt, err = time.Parse(time.RFC3339, updatedAt)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: parsing updated_at: %w", err)
	}
	return &user, nil
}

func marshalIntegrationToken(token *gestalt.StoredIntegrationToken, paramsJSON string) map[string]ddbtypes.AttributeValue {
	item := map[string]ddbtypes.AttributeValue{
		attrPK:                &ddbtypes.AttributeValueMemberS{Value: userPKPrefix + token.UserID},
		attrSK:                &ddbtypes.AttributeValueMemberS{Value: tokenSKPrefix + token.Integration + "#" + token.Connection + "#" + token.Instance},
		attrID:                &ddbtypes.AttributeValueMemberS{Value: token.ID},
		attrUserID:            &ddbtypes.AttributeValueMemberS{Value: token.UserID},
		attrIntegration:       &ddbtypes.AttributeValueMemberS{Value: token.Integration},
		attrConnection:        &ddbtypes.AttributeValueMemberS{Value: token.Connection},
		attrInstance:          &ddbtypes.AttributeValueMemberS{Value: token.Instance},
		attrAccessTokenEnc:    &ddbtypes.AttributeValueMemberS{Value: string(token.AccessTokenSealed)},
		attrRefreshTokenEnc:   &ddbtypes.AttributeValueMemberS{Value: string(token.RefreshTokenSealed)},
		attrScopes:            &ddbtypes.AttributeValueMemberS{Value: token.Scopes},
		attrRefreshErrorCount: &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(int64(token.RefreshErrorCount), 10)},
		attrMetadataJSON:      &ddbtypes.AttributeValueMemberS{Value: paramsJSON},
		attrCreatedAt:         &ddbtypes.AttributeValueMemberS{Value: token.CreatedAt.Format(time.RFC3339)},
		attrUpdatedAt:         &ddbtypes.AttributeValueMemberS{Value: token.UpdatedAt.Format(time.RFC3339)},
	}
	if token.ExpiresAt != nil {
		item[attrExpiresAt] = &ddbtypes.AttributeValueMemberS{Value: token.ExpiresAt.Format(time.RFC3339)}
	}
	if token.LastRefreshedAt != nil {
		item[attrLastRefreshedAt] = &ddbtypes.AttributeValueMemberS{Value: token.LastRefreshedAt.Format(time.RFC3339)}
	}
	return item
}

func unmarshalIntegrationToken(item map[string]ddbtypes.AttributeValue) (*gestalt.StoredIntegrationToken, error) {
	var (
		token            gestalt.StoredIntegrationToken
		accessToken      string
		refreshToken     string
		createdAt        string
		updatedAt        string
		lastRefreshed    string
		refreshCount     string
		connectionParams string
	)

	if err := unmarshalString(item, attrID, &token.ID); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrUserID, &token.UserID); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrIntegration, &token.Integration); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrConnection, &token.Connection); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrInstance, &token.Instance); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrAccessTokenEnc, &accessToken); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrRefreshTokenEnc, &refreshToken); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrScopes, &token.Scopes); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrMetadataJSON, &connectionParams); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrCreatedAt, &createdAt); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrUpdatedAt, &updatedAt); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrLastRefreshedAt, &lastRefreshed); err != nil {
		return nil, err
	}
	if value, ok := item[attrRefreshErrorCount]; ok {
		if err := attributevalue.Unmarshal(value, &refreshCount); err != nil {
			return nil, fmt.Errorf("dynamodb: unmarshalling refresh_error_count: %w", err)
		}
		if refreshCount != "" {
			parsed, err := strconv.ParseInt(refreshCount, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("dynamodb: parsing refresh_error_count: %w", err)
			}
			token.RefreshErrorCount = int32(parsed)
		}
	}

	params, err := connectionParamsFromJSON(connectionParams)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: decode connection params: %w", err)
	}
	token.AccessTokenSealed = []byte(accessToken)
	token.RefreshTokenSealed = []byte(refreshToken)
	token.ConnectionParams = params

	token.CreatedAt, err = time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: parsing created_at: %w", err)
	}
	token.UpdatedAt, err = time.Parse(time.RFC3339, updatedAt)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: parsing updated_at: %w", err)
	}
	if lastRefreshed != "" {
		parsed, err := time.Parse(time.RFC3339, lastRefreshed)
		if err != nil {
			return nil, fmt.Errorf("dynamodb: parsing last_refreshed_at: %w", err)
		}
		token.LastRefreshedAt = &parsed
	}
	if value, ok := item[attrExpiresAt]; ok {
		var expiresAt string
		if err := attributevalue.Unmarshal(value, &expiresAt); err != nil {
			return nil, fmt.Errorf("dynamodb: unmarshalling expires_at: %w", err)
		}
		if expiresAt != "" {
			parsed, err := time.Parse(time.RFC3339, expiresAt)
			if err != nil {
				return nil, fmt.Errorf("dynamodb: parsing expires_at: %w", err)
			}
			token.ExpiresAt = &parsed
		}
	}
	return &token, nil
}

func marshalAPIToken(token *gestalt.StoredAPIToken) map[string]ddbtypes.AttributeValue {
	item := map[string]ddbtypes.AttributeValue{
		attrPK:          &ddbtypes.AttributeValueMemberS{Value: userPKPrefix + token.UserID},
		attrSK:          &ddbtypes.AttributeValueMemberS{Value: apiTokenSKPrefix + token.ID},
		attrID:          &ddbtypes.AttributeValueMemberS{Value: token.ID},
		attrUserID:      &ddbtypes.AttributeValueMemberS{Value: token.UserID},
		attrName:        &ddbtypes.AttributeValueMemberS{Value: token.Name},
		attrHashedToken: &ddbtypes.AttributeValueMemberS{Value: token.HashedToken},
		attrScopes:      &ddbtypes.AttributeValueMemberS{Value: token.Scopes},
		attrCreatedAt:   &ddbtypes.AttributeValueMemberS{Value: token.CreatedAt.Format(time.RFC3339)},
		attrUpdatedAt:   &ddbtypes.AttributeValueMemberS{Value: token.UpdatedAt.Format(time.RFC3339)},
	}
	if token.ExpiresAt != nil {
		item[attrExpiresAt] = &ddbtypes.AttributeValueMemberS{Value: token.ExpiresAt.Format(time.RFC3339)}
	}
	return item
}

func unmarshalAPIToken(item map[string]ddbtypes.AttributeValue) (*gestalt.StoredAPIToken, error) {
	var (
		token     gestalt.StoredAPIToken
		createdAt string
		updatedAt string
	)
	if err := unmarshalString(item, attrID, &token.ID); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrUserID, &token.UserID); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrName, &token.Name); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrHashedToken, &token.HashedToken); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrScopes, &token.Scopes); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrCreatedAt, &createdAt); err != nil {
		return nil, err
	}
	if err := unmarshalString(item, attrUpdatedAt, &updatedAt); err != nil {
		return nil, err
	}

	var err error
	token.CreatedAt, err = time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: parsing created_at: %w", err)
	}
	token.UpdatedAt, err = time.Parse(time.RFC3339, updatedAt)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: parsing updated_at: %w", err)
	}
	if value, ok := item[attrExpiresAt]; ok {
		var expiresAt string
		if err := attributevalue.Unmarshal(value, &expiresAt); err != nil {
			return nil, fmt.Errorf("dynamodb: unmarshalling expires_at: %w", err)
		}
		if expiresAt != "" {
			parsed, err := time.Parse(time.RFC3339, expiresAt)
			if err != nil {
				return nil, fmt.Errorf("dynamodb: parsing expires_at: %w", err)
			}
			token.ExpiresAt = &parsed
		}
	}
	return &token, nil
}

func unmarshalString(item map[string]ddbtypes.AttributeValue, key string, dest *string) error {
	value, ok := item[key]
	if !ok {
		*dest = ""
		return nil
	}
	return attributevalue.Unmarshal(value, dest)
}

func connectionParamsToJSON(values map[string]string) (string, error) {
	if len(values) == 0 {
		return "", nil
	}
	data, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func connectionParamsFromJSON(value string) (map[string]string, error) {
	if value == "" {
		return nil, nil
	}
	var out map[string]string
	if err := json.Unmarshal([]byte(value), &out); err != nil {
		return nil, err
	}
	return out, nil
}
