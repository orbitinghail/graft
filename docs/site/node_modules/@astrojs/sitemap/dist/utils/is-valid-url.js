const isValidUrl = (s) => {
  if (typeof s !== "string" || !s) {
    return false;
  }
  try {
    new URL(s);
    return true;
  } catch {
    return false;
  }
};
export {
  isValidUrl
};
