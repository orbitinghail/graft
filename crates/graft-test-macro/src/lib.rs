use std::path::PathBuf;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse::Parse, parse_macro_input, Ident, ItemFn, LitStr, Token};

#[proc_macro_attribute]
pub fn test(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemFn);
    let ItemFn { attrs, vis, sig, block } = input;

    // automatic async detection!
    let test_attr = if sig.asyncness.is_some() {
        quote!(#[tokio::test(start_paused = true)])
    } else {
        quote!(#[test])
    };

    let statements = block.stmts;

    quote!(
        #(#attrs)*
        #test_attr
        #vis #sig {
            graft_test::setup_test();
            #(#statements)*
        }
    )
    .into()
}

struct DatatestArgs {
    runner: syn::Path,
    glob: syn::LitStr,
}

impl Parse for DatatestArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let runner = input.parse()?;
        input.parse::<Token![,]>()?;
        let glob = input.parse()?;
        Ok(Self { runner, glob })
    }
}

/// Maps a runner function over a glob of paths. Each matching file will
/// generate a separate rust `#[test]` named after the basename of the file
/// (excluding the extension).
///
/// Example:
///
/// ```
/// fn runner(path: &std::path::Path) {
///   println!("testing {path:?}");
/// }
///
/// datatest!(runner, "tests/**/*.sql");
/// ```
#[proc_macro]
pub fn datatest(args: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as DatatestArgs);

    macro_rules! mkerr {
        ($msg:expr) => {
            syn::Error::new(Span::call_site(), $msg.to_string())
        };
    }

    fn mktest(runner: &syn::Path, path: PathBuf) -> syn::Result<proc_macro2::TokenStream> {
        let path = path.canonicalize().map_err(|err| mkerr!(err))?;
        let lit_path = LitStr::new(&path.as_os_str().to_string_lossy(), Span::call_site());

        // derive test name from path
        let stem = path
            .file_stem()
            .ok_or(mkerr!(format!("invalid path `{path:?}`")))?
            .to_string_lossy();
        let test_name = Ident::new(&stem, Span::call_site());

        Ok(quote! {
            #[graft_test::test]
            fn #test_name() {
                let path = #lit_path;
                #runner(&std::path::Path::new(&path))
            }

        })
    }

    fn datatest_inner(args: DatatestArgs) -> syn::Result<proc_macro2::TokenStream> {
        let paths = glob::glob(&args.glob.value()).map_err(|err| mkerr!(err))?;
        let mut tests = vec![];
        for entry in paths {
            match entry {
                Ok(path) => tests.push(mktest(&args.runner, path)?),
                Err(err) => return Err(mkerr!(err)),
            }
        }

        Ok(quote! {
            #(#tests)*
        })
    }

    datatest_inner(args)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
