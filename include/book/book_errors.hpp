#pragma once

#include <string_view>

namespace mdh::book {

enum class BookError {
    UnknownOrderId,   // cancel/modify referenced an order id we don't have
    DuplicateOrderId, // add referenced an order id we already have (for this instrument)
    InvalidPrice,     // price <= 0
    InvalidQuantity,  // quantity == 0
};

[[nodiscard]] constexpr std::string_view to_string(BookError e) {
    switch (e) {
        case BookError::UnknownOrderId:   return "UnknownOrderId";
        case BookError::DuplicateOrderId: return "DuplicateOrderId";
        case BookError::InvalidPrice:     return "InvalidPrice";
        case BookError::InvalidQuantity:  return "InvalidQuantity";
    }
    return "UnknownBookError";
}

} // namespace mdh::book
