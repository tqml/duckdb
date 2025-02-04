

@static if VERSION <= v"1.7"

    # 
    keytype(t) = eltype(keys(t))

end
