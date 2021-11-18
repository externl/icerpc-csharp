// Copyright (c) ZeroC, Inc. All rights reserved.

use slice::code_gen_util::TypeContext;
use slice::grammar::*;

use super::entity_ext::EntityExt;
use super::interface_ext::InterfaceExt;
use super::primitive_ext::PrimitiveExt;

pub trait TypeRefExt {
    /// Is the type a value type (eg. Struct)
    fn is_value_type(&self) -> bool;

    /// The C# mapped type for this type reference.
    fn to_type_string(
        &self,
        namespace: &str,
        context: TypeContext,
        ignore_optional: bool,
    ) -> String;
}

impl<T: Type + ?Sized> TypeRefExt for TypeRef<T> {
    fn is_value_type(&self) -> bool {
        match self.concrete_type() {
            Types::Primitive(primitive) => {
                !matches!(primitive, Primitive::String | Primitive::AnyClass)
            }
            Types::Enum(_) | Types::Struct(_) | Types::Interface(_) => true,
            _ => false,
        }
    }

    fn to_type_string(
        &self,
        namespace: &str,
        context: TypeContext,
        mut ignore_optional: bool,
    ) -> String {
        let type_str = match &self.concrete_typeref() {
            TypeRefs::Struct(struct_ref) => struct_ref.escape_scoped_identifier(namespace),
            TypeRefs::Class(class_ref) => class_ref.escape_scoped_identifier(namespace),
            TypeRefs::Enum(enum_ref) => enum_ref.escape_scoped_identifier(namespace),
            TypeRefs::Interface(interface_ref) => {
                interface_ref.scoped_proxy_implementation_name(namespace)
            }
            TypeRefs::Sequence(sequence_ref) => {
                sequence_type_to_string(sequence_ref, namespace, context)
            }
            TypeRefs::Dictionary(dictionary_ref) => {
                dictionary_type_to_string(dictionary_ref, namespace, context)
            }
            TypeRefs::Primitive(primitive_ref) => primitive_ref.cs_keyword().to_owned(),
        };

        // For readonly sequences of fixed size numeric elements the mapping is the
        // same for optional an non optional types.
        if let Types::Sequence(sequence) = self.concrete_type() {
            if context == TypeContext::Outgoing
                && sequence.has_fixed_size_numeric_elements()
                && !self.has_attribute("cs:generic", false)
            {
                ignore_optional = true;
            }
        }

        if self.is_optional && !ignore_optional {
            type_str + "?"
        } else {
            type_str
        }
    }
}

/// Helper method to convert a sequence type into a string
fn sequence_type_to_string(
    sequence_ref: &TypeRef<Sequence>,
    namespace: &str,
    context: TypeContext,
) -> String {
    let element_type =
        sequence_ref
            .element_type
            .to_type_string(namespace, TypeContext::Nested, false);

    match context {
        TypeContext::DataMember | TypeContext::Nested => {
            format!("global::System.Collections.Generic.IList<{}>", element_type)
        }
        TypeContext::Incoming => match sequence_ref.get_attribute("cs:generic", false) {
            Some(args) => match args.first().unwrap().as_str() {
                value @ "List" | value @ "LinkedList" | value @ "Queue" | value @ "Stack" => {
                    format!(
                        "global::System.Collections.Generic.{}<{}>",
                        value, element_type
                    )
                }
                value => format!("{}<{}>", value, element_type),
            },
            None => format!("{}[]", element_type),
        },
        TypeContext::Outgoing => {
            // If the underlying type is of fixed size, we map to `ReadOnlyMemory` instead.
            if sequence_ref.has_fixed_size_numeric_elements()
                && !sequence_ref.has_attribute("cs:generic", false)
            {
                format!("global::System.ReadOnlyMemory<{}>", element_type)
            } else {
                format!(
                    "global::System.Collections.Generic.IEnumerable<{}>",
                    element_type
                )
            }
        }
    }
}

/// Helper method to convert a dictionary type into a string
fn dictionary_type_to_string(
    dictionary_ref: &TypeRef<Dictionary>,
    namespace: &str,
    context: TypeContext,
) -> String {
    let key_type = dictionary_ref
        .key_type
        .to_type_string(namespace, TypeContext::Nested, false);
    let value_type =
        dictionary_ref
            .value_type
            .to_type_string(namespace, TypeContext::Nested, false);

    match context {
        TypeContext::DataMember | TypeContext::Nested => {
            format!(
                "global::System.Collections.Generic.IDictionary<{}, {}>",
                key_type, value_type,
            )
        }
        TypeContext::Incoming => match dictionary_ref.get_attribute("cs:generic", false) {
            Some(args) => {
                let prefix = match args.first().unwrap().as_str() {
                    "SortedDictionary" => "global::System.Collections.Generic.",
                    _ => "",
                };
                format!(
                    "{}{}<{}, {}>",
                    prefix,
                    args.first().unwrap(),
                    key_type,
                    value_type
                )
            }
            None => format!(
                "global::System.Collections.Generic.Dictionary<{}, {}>",
                key_type, value_type,
            ),
        },
        TypeContext::Outgoing => {
            format!(
                "global::System.Collections.Generic.IEnumerable<global::System.Collections.Generic.KeyValuePair<{}, {}>>",
                key_type, value_type,
            )
        }
    }
}