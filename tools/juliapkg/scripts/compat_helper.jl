using CompatHelper

main(
    env::AbstractDict=ENV,
    ci_cfg::CIService=auto_detect_ci_service(; env=env);
    entry_type::EntryType=KeepEntry(),
    registries::Vector{Pkg.RegistrySpec}=DEFAULT_REGISTRIES,
    use_existing_registries::Bool=false,
    depot::String=DEPOT_PATH[1],
    subdirs::AbstractVector{<:AbstractString}=[""],
    master_branch::Union{DefaultBranch,AbstractString}=DefaultBranch(),
    bump_compat_containing_equality_specifier=true,
    pr_title_prefix::String="",
    include_jll::Bool=false,
    unsub_from_prs=false,
    cc_user=false,
    bump_version=false,
    include_yanked=false,
)