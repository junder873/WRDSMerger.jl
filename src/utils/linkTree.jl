
"""
This section relies heavily on AbstractTrees.jl
The goal is to build a tree structure starting from
an arbitrary type to all other defined types.
Once this tree is created, it is relatively easy to
go back and find where I want to get on the tree
(basically, the end type) and see the tables necessary
to get there.
"""

mutable struct FirmIdentifierNode
    
    data::Type{<:FirmIdentifier}
    parent::FirmIdentifierNode
    children::Vector{FirmIdentifierNode}
    
    # Root constructor
    FirmIdentifierNode(data) = new(data)
    # child node constructor
    FirmIdentifierNode(data, parent::FirmIdentifierNode) = new(data, parent)
end

function addchild(data, parent::FirmIdentifierNode)
    if !isdefined(parent, :children)
        parent.children = FirmIdentifierNode[]
    end
    push!(parent.children, FirmIdentifierNode(data, parent))
end

function AbstractTrees.children(node::FirmIdentifierNode)
    return node
    #isdefined(node, :children) && node.children
    # return node.children
end

AbstractTrees.printnode(io::IO, node::FirmIdentifierNode) = print(io, node.data)

function Base.iterate(node::FirmIdentifierNode, state::Int=1)
    isdefined(node, :children) && length(node.children) >= state && return (node.children[state], state+1)
    return nothing
end

Base.IteratorSize(::Type{FirmIdentifierNode}) = Base.SizeUnknown()
Base.eltype(::Type{FirmIdentifierNode}) = FirmIdentifierNode

Base.IteratorEltype(::Type{<:TreeIterator{FirmIdentifierNode}}) = Base.HasEltype()
AbstractTrees.parentlinks(::Type{FirmIdentifierNode}) = AbstractTrees.StoredParents()
AbstractTrees.siblinglinks(::Type{FirmIdentifierNode}) = AbstractTrees.StoredSiblings()


Base.parent(root::FirmIdentifierNode, node::FirmIdentifierNode) = isdefined(node, :parent) ? node.parent : nothing
Base.parent(node::FirmIdentifierNode) = isdefined(node, :parent) ? node.parent : nothing

Base.pairs(node::FirmIdentifierNode) = enumerate(node)

##

"""
These are the functions necessary to construct the tree
"""

function get_root(node::FirmIdentifierNode)
    if parent(node) !== nothing
        return get_root(parent(node))
    end
    return node
end

function existing_types(
    node::FirmIdentifierNode;
    out=Type{<:FirmIdentifier}[]
)

    push!(out, node.data)
    for x in node
        push!(out, x.data)
        existing_types(x; out)
    end

    out
end


function base_firm_identifiers_types(
    x::Type{<:FirmIdentifier};
    out=Type{<:FirmIdentifier}[]
)
    for y in subtypes(x)
        if isabstracttype(y)
            out=base_firm_identifiers_types(y; out)
        else
            push!(out, y)
        end
    end

    out |> unique
end

function build_tree_base(x::FirmIdentifierNode)
    for y in base_firm_identifiers_types(FirmIdentifier)

        
        if isabstracttype(y) || y == x.data || y ∈ existing_types(get_root(x))
            continue
        end

        try
            LinkTable(x.data, y)
            addchild(y, x)
        catch
            continue
        end
    end
    if isdefined(x, :children)
        for y in x
            build_tree_base(y)
        end
    end
end

function build_tree(x::Type{<:FirmIdentifier})
    root = FirmIdentifierNode(x)
    build_tree_base(root)
    return root
end