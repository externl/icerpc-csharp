// Copyright (c) ZeroC, Inc.

using NUnit.Framework;

namespace Slice.Tests.TypeIdAttributeTestNamespace;

public sealed class TypeIdAttributeTests
{
    /// <summary>Provides test case data for the <see cref="Get_all_slice_type_ids" /> test.</summary>
    private static IEnumerable<TestCaseData> GetAllSliceTypeIdsSource
    {
        get
        {
            yield return new TestCaseData(
                typeof(MyDerivedClass),
                new string[]
                {
                    "::Slice::Tests::TypeIdAttributeTestNamespace::MyDerivedClass",
                    "::Slice::Tests::TypeIdAttributeTestNamespace::MyClass"
                });
        }
    }

    /// <summary>Verifies that types generated from Slice definitions have the expected type ID.</summary>
    /// <param name="type">The <see cref="Type" /> of the generated type to test.</param>
    /// <param name="expected">The expected type ID.</param>
    [TestCase(typeof(MyClass), "::Slice::Tests::TypeIdAttributeTestNamespace::MyClass")]
    [TestCase(typeof(MyException), null)] // Slice2 exception
    [TestCase(typeof(MyOtherClass), "::Slice::Tests::TypeIdAttributeTestNamespace::myOtherClass")]
    [TestCase(typeof(MyOtherException), null)] // Slice2 exception
    public void Get_slice_type_id(Type type, string? expected)
    {
        string? typeId = type.GetSliceTypeId();
        Assert.That(typeId, Is.EqualTo(expected));
    }

    [Test, TestCaseSource(nameof(GetAllSliceTypeIdsSource))]
    public void Get_all_slice_type_ids(Type type, string[] expected)
    {
        string[] typeIds = type.GetAllSliceTypeIds();
        Assert.That(typeIds, Is.EqualTo(expected));
    }
}