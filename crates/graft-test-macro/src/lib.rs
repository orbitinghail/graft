use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

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
