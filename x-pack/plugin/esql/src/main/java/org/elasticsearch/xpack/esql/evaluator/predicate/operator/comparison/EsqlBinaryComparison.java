/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison;

import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

public abstract class EsqlBinaryComparison extends BinaryComparison implements EvaluatorMapper {

    private final Map<DataType, BinaryEvaluator> evaluatorMap;

    protected EsqlBinaryComparison(
        Source source,
        Expression left,
        Expression right,
        /* TODO: BinaryComparisonOperator is an enum with a bunch of functionality we don't really want. We should extract an interface and
                 create a symbol only version like we did for BinaryArithmeticOperation. Ideally, they could be the same class.
         */
        BinaryComparisonProcessor.BinaryComparisonOperation operation,
        Map<DataType, BinaryEvaluator> evaluatorMap
    ) {
        this(source, left, right, operation, null, evaluatorMap);
    }

    protected EsqlBinaryComparison(
        Source source,
        Expression left,
        Expression right,
        BinaryComparisonProcessor.BinaryComparisonOperation operation,
        // TODO: We are definitely not doing the right thing with this zoneId
        ZoneId zoneId,
        Map<DataType, BinaryEvaluator> evaluatorMap
    ) {
        super(source, left, right, operation, zoneId);
        this.evaluatorMap = evaluatorMap;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        // Our type is always boolean, so figure out the evaluator type from the inputs
        DataType commonType = EsqlDataTypeRegistry.INSTANCE.commonType(left().dataType(), right().dataType());
        var lhs = Cast.cast(source(), left().dataType(), commonType, toEvaluator.apply(left()));
        var rhs = Cast.cast(source(), right().dataType(), commonType, toEvaluator.apply(right()));

        if (evaluatorMap.containsKey(commonType) == false) {
            throw new EsqlIllegalArgumentException("Unsupported type " + left().dataType());
        }
        return evaluatorMap.get(commonType).apply(source(), lhs, rhs);
    }

    @Override
    public Boolean fold() {
        return (Boolean) EvaluatorMapper.super.fold();
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution typeResolution = super.resolveType();
        if (typeResolution.unresolved()) {
            return typeResolution;
        }

        return checkCompatibility();
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, TypeResolutions.ParamOrdinal paramOrdinal) {
        return TypeResolutions.isType(
            e,
            evaluatorMap::containsKey,
            sourceText(),
            paramOrdinal,
            evaluatorMap.keySet().stream().map(DataType::typeName).toArray(String[]::new)
        );
    }

    /**
     * Check if the two input types are compatible for this operation
     *
     * @return TypeResolution.TYPE_RESOLVED iff the types are compatible.  Otherwise, an appropriate type resolution error.
     */
    protected TypeResolution checkCompatibility() {
        DataType leftType = left().dataType();
        DataType rightType = right().dataType();

        // Unsigned long is only interoperable with other unsigned longs
        if ((rightType == UNSIGNED_LONG && (false == (leftType == UNSIGNED_LONG || leftType == DataTypes.NULL)))
            || (leftType == UNSIGNED_LONG && (false == (rightType == UNSIGNED_LONG || rightType == DataTypes.NULL)))) {
            return new TypeResolution(EsqlArithmeticOperation.formatIncompatibleTypesMessage(symbol(), leftType, rightType));
        }

        if ((leftType.isNumeric() && rightType.isNumeric())
            || leftType.equals(rightType)
            || DataTypes.isNull(leftType)
            || DataTypes.isNull(rightType)) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return new TypeResolution(EsqlArithmeticOperation.formatIncompatibleTypesMessage(symbol(), leftType, rightType));
    }

    // NOCOMMIT: This is the same as EsqlArithmeticOperation#ArithmeticEvaluator, and they should be refactored to the same place
    @FunctionalInterface
    interface BinaryEvaluator {
        EvalOperator.ExpressionEvaluator.Factory apply(
            Source source,
            EvalOperator.ExpressionEvaluator.Factory lhs,
            EvalOperator.ExpressionEvaluator.Factory rhs
        );
    }
}
